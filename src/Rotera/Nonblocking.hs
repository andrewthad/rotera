{-# language UnboxedTuples #-}
{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language MultiWayIf #-}
{-# language ScopedTypeVariables #-}

module Rotera.Nonblocking
  ( read
  , readMany
  , with
  ) where

import Prelude hiding (read)

import Control.Concurrent.STM (TVar)
import Data.ByteString (ByteString)
import Data.Primitive (ByteArray,MutableByteArray(..))
import Data.Primitive.Addr (Addr(..))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import GHC.Exts (Ptr(..),MutableByteArray#,Addr#,RealWorld,touch#)
import GHC.IO (IO(..))
import Rotera.Unsafe (Rotera(..))
import Rotera.Unsafe (nonblockingLockEvent,unlockEvent)

import qualified Data.ByteString.Internal as BSI
import qualified Data.Primitive as PM
import qualified Data.Primitive.Addr as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified GHC.ForeignPtr as FP

readMany ::
     Rotera
  -> Int -- ^ Starting message ID
  -> Int -- ^ Maximum number of events
  -> IO (Int,UnliftedArray ByteArray)
readMany r reqId maxEvts = do
  lenBuf <- PM.newPrimArray maxEvts
  res <- PM.unsafeNewUnliftedArray maxEvts
  (actualEventId,totalEvts) <- with r reqId maxEvts (0 :: Int)
    (\lenIx len -> do
      debug $ "readMany: written length is " ++ show len
      PM.writePrimArray lenBuf lenIx len
      pure (lenIx + 1)
    )
    (\totalEvts addr _ _ -> do
      let go !ix !boff = if ix < totalEvts
            then do
              len <- PM.readPrimArray lenBuf ix
              debug $ "readMany: read length is " ++ show len
              buf <- PM.newByteArray len
              PM.copyAddrToByteArray buf 0 (PM.plusAddr addr boff) len
              buf' <- PM.unsafeFreezeByteArray buf
              PM.writeUnliftedArray res ix buf'
              go (ix + 1) (boff + len)
            else pure ()
      go 0 0
    )
  res' <- if totalEvts < maxEvts
    then do
      x <- PM.unsafeFreezeUnliftedArray res
      PM.unsafeFreezeUnliftedArray =<< PM.thawUnliftedArray x 0 totalEvts
    else PM.unsafeFreezeUnliftedArray res
  pure (actualEventId,res')

-- | Read a event from the queue. If the event identifier refers to an
--   event that has already been purged, the oldest persisted event is returned
--   along with its identifier. If the event identifier refers to an event
--   that has not yet happened, the newest persisted event is returned along
--   with its identifier. This function is thread-safe.
--
--   This only reads committed events.
read ::
     Rotera -- ^ rotating queue
  -> Int -- ^ event identifier
  -> IO (Maybe (Int,ByteString))
read r@(Rotera base maxEventBytes maximumEvents _ _ discourseVar) !requestedEventId = do
  debug $ "read: maximum events is " ++ show maximumEvents
  (actualEventId,actualEvts,_) <- nonblockingLockEvent discourseVar requestedEventId 1
  if | actualEvts == 1 -> do
         debug $ "read: actualEventId is " ++ show actualEventId
         (off,offNext) <- calculateOffsets r actualEventId 1
         (dst,len) <- if offNext >= off
           then do
             let len = offNext - off
                 addrA = (PM.plusAddr base (4096 + off))
             dst <- PM.newPinnedByteArray len
             PM.copyAddrToByteArray dst 0 addrA len
             pure (dst,len)
           else do
             let len = maxEventBytes + (offNext - off)
                 lenA = (maxEventBytes - off)
                 lenB = offNext
                 addrA = (PM.plusAddr base (4096 + off))
                 addrB = (PM.plusAddr base 4096)
             dst <- PM.newPinnedByteArray len
             PM.copyAddrToByteArray dst 0 addrA lenA
             PM.copyAddrToByteArray dst lenA addrB lenB
             pure (dst,len)
         unlockEvent discourseVar actualEventId
         let !res = BSI.PS (FP.ForeignPtr (unPtr (PM.mutableByteArrayContents dst)) (FP.PlainPtr (unMutableByteArray dst))) 0 len
         pure (Just (actualEventId,res))
     | actualEvts == 0 -> pure Nothing
     | otherwise -> fail "Rotera.Nonblocking.read: should not happen"

calculateOffsets :: Rotera -> Int -> Int -> IO (Int,Int)
calculateOffsets (Rotera base maxEventBytes maximumEvents _ _ _) eventId maxEvts = do
  let modEventId = (mod eventId maximumEvents)
  off <- PM.readOffAddr
    (PM.plusAddr base (4096 + maxEventBytes))
    modEventId
  let modEventIdSucc = (mod (eventId + maxEvts) maximumEvents)
  offNext <- PM.readOffAddr
    (PM.plusAddr base (4096 + maxEventBytes))
    modEventIdSucc
  pure (off,offNext)

-- This function does not mask exceptions. The callback is expected to not
-- throw exceptions.
with ::
     Rotera
  -> Int -- starting event id
  -> Int -- maximum number of events
  -> a -- initial length-fold accumulator
  -> (a -> Int -> IO a)
     -- callback for folding over the payloads lengths, called many times
  -> (Int -> Addr -> Int -> TVar Bool -> IO ())
     -- Payload-copying callback, called once.
  -> IO (Int,Int)
with r@(Rotera base maxEventBytes maximumEvents _ _ discourseVar) !requestedEventId !maxEvts !a0 g f = do
  (actualEventId,actualEvts,signal) <- nonblockingLockEvent discourseVar requestedEventId maxEvts
  if | actualEvts > 0 -> do
         offPrev0 <- PM.readOffAddr (PM.plusAddr base (4096 + maxEventBytes))
           (mod actualEventId maximumEvents)
         let lastEventSucc = actualEventId + actualEvts
         let go !a !offPrev !evtId = if evtId <= lastEventSucc
               then do
                 off <- PM.readOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod evtId maximumEvents)
                 let !rawDelta = off - offPrev
                 a' <- g a (if rawDelta >= 0 then rawDelta else maxEventBytes + rawDelta)
                 go a' off (evtId + 1)
               else pure ()
         go a0 offPrev0 (actualEventId + 1)
         (off,offNext) <- calculateOffsets r actualEventId actualEvts
         if offNext >= off
           then do
             let len = offNext - off
                 addrA = (PM.plusAddr base (4096 + off))
             f actualEvts addrA len signal
           else do
             let len = maxEventBytes + (offNext - off)
                 lenA = (maxEventBytes - off)
                 lenB = offNext
                 addrA = (PM.plusAddr base (4096 + off))
                 addrB = (PM.plusAddr base 4096)
             dst <- PM.newPinnedByteArray len
             PM.copyAddrToByteArray dst 0 addrA lenA
             PM.copyAddrToByteArray dst lenA addrB lenB
             let addr = ptrToAddr (PM.mutableByteArrayContents dst)
             f actualEvts addr len signal
             touchMutableByteArray dst
         unlockEvent discourseVar actualEventId
         pure (actualEventId,actualEvts)
     | actualEvts == 0 -> pure (requestedEventId,0)
     | otherwise -> fail "rotera.with: should not happen"

debug :: String -> IO ()
debug _ = pure ()

unMutableByteArray :: MutableByteArray s -> MutableByteArray# s
unMutableByteArray (PM.MutableByteArray x) = x

unPtr :: Ptr a -> Addr#
unPtr (Ptr x) = x

ptrToAddr :: Ptr a -> Addr
ptrToAddr (Ptr x) = Addr x

touchMutableByteArray :: MutableByteArray RealWorld -> IO ()
touchMutableByteArray (MutableByteArray x) = touchMutableByteArray# x

touchMutableByteArray# :: MutableByteArray# RealWorld -> IO ()
touchMutableByteArray# x = IO $ \s -> case touch# x s of s' -> (# s', () #)

