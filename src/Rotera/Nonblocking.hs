{-# language BangPatterns #-}
{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language MultiWayIf #-}
{-# language NamedFieldPuns #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language UnboxedTuples #-}
{-# language OverloadedStrings #-}
{-# language GADTs #-}

module Rotera.Nonblocking
  ( read
  , readMany
  , readIntoSocket
  -- , with
  ) where

import Prelude hiding (read)

import Control.Concurrent.STM (TVar)
import Data.Bytes.Types (MutableBytes(..))
import Data.ByteString (ByteString)
import Data.Primitive (ByteArray,MutableByteArray(..),MutablePrimArray(..))
import Data.Primitive (SmallArray)
import Data.Primitive.Addr (Addr(..))
import Data.Primitive (Prim)
import Data.Word (Word32,Word64)
import Socket.Stream.IPv4 (Connection,SendException(..),Interruptibility(..))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import GHC.Exts (Ptr(..),MutableByteArray#,Addr#,RealWorld,touch#)
import GHC.IO (IO(..))
import Rotera.Unsafe (Rotera(..),NonblockingResult(..),ReadTicket)
import Rotera.Unsafe (nonblockingLockEvent,removeReadTicket)
import System.ByteOrder (Fixed(..),ByteOrder(LittleEndian))

import qualified Data.Bytes.Unsliced as BU
import qualified Data.ByteString.Char8 as BC
import qualified Data.Primitive.Ptr as PM
import qualified Data.ByteString.Internal as BSI
import qualified Data.Primitive as PM
import qualified Data.Primitive.Addr as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified GHC.ForeignPtr as FP
import qualified Control.Concurrent.STM as STM
import qualified Socket.Stream.Interruptible.Addr as SAI
import qualified Socket.Stream.Uninterruptible.MutableBytes as SMBU

-- Precondition: The events buffer has three 64-bit slots in
-- the beginning. The first one is already filled out with
-- the aliveness. The second one needs to be filled in with
-- the first message id. The third one needs to be filled in
-- with the number of messages in this batch. The remainder of
-- the array is enough 32-bit slots to hold all the lengths.
readIntoSocket ::
     Rotera
  -> ByteArray -- peer description
  -> Int -- minimum message id
  -> Int -- maximum number of messages
  -> Connection -- connection to peer
  -> MutableByteArray RealWorld
     -- ^ reusable msg-length buffer, the 
  -> IO (Maybe Int) -- number of messages, or Nothing if error
readIntoSocket r@Rotera{base,activeReadersVar,maxEventBytes} descr reqId maxEvts conn lenBuf = do
  nonblockingLockEvent r reqId maxEvts >>= \case
    -- Rethink this. If someone uses the blocking interface,
    -- this first case cannot actually happen. It may be better
    -- to have a dedicate blocking read and nonblocking read
    -- in Rotera.Socket instead of having this single function
    -- here. Then we could also be much more aggresive with
    -- recursion.
    NonblockingResultNothing latestEventId -> do
      PM.writeByteArray lenBuf 1
        (Fixed (fromIntegral latestEventId) :: Fixed 'LittleEndian Word64)
      PM.writeByteArray lenBuf 2
        (Fixed 0 :: Fixed 'LittleEndian Word64)
      SMBU.send conn (MutableBytes lenBuf 0 24) >>= \case
        Left err -> Nothing <$ documentSendException descr err
        Right _ -> pure (Just 0)
    NonblockingResultSomething actualEventId actualEvts signal -> do
      PM.writeByteArray lenBuf 1
        (Fixed (fromIntegral actualEventId) :: Fixed 'LittleEndian Word64)
      PM.writeByteArray lenBuf 2
        (Fixed (fromIntegral actualEvts) :: Fixed 'LittleEndian Word64)
      let lastEventSucc = actualEventId + actualEvts
      totalBytes <- copyOffsets r
        (typed @(Fixed 'LittleEndian Word32) lenBuf) actualEventId lastEventSucc 6
      (off,offNext) <- calculateOffsets r actualEventId actualEvts
      (!addr,!x) <- mergePossibleSplit base lenBuf maxEventBytes off offNext
      -- TODO: It is tempting to try to merge these two with
      -- sendmsg. However, it makes handling the signal more
      -- difficult.
      SMBU.send conn (MutableBytes lenBuf 0 (24 + actualEvts * 4)) >>= \case
        Left err -> Nothing <$ documentSendException descr err
        Right _ -> SAI.send signal conn addr totalBytes >>= \case
          Left err -> case err of
            SendShutdown -> Nothing <$ printPrefixed descr
              "[warn] Client closed connection while server was sending messages.\n"
            SendReset -> Nothing <$ printPrefixed descr
              "[warn] Client reset connection while server was sending messages.\n"
            SendInterrupted transmitted -> do
              printPrefixed descr
                "[info] Using anciliary memory for client reading expiring messages.\n"
              let remaining = totalBytes - transmitted
              buf <- PM.newByteArray remaining
              PM.copyAddrToByteArray buf 0 (PM.plusAddr addr transmitted) remaining
              touchMutableByteArray x
              unlockEvent signal activeReadersVar
              SMBU.send conn (MutableBytes buf 0 remaining) >>= \case
                Left errX -> case errX of
                  -- TODO: deduplicate
                  SendShutdown -> Nothing <$ printPrefixed descr
                    "[warn] Client closed connection while server was sending messages.\n"
                  SendReset -> Nothing <$ printPrefixed descr
                    "[warn] Client reset connection while server was sending messages.\n"
                Right _ -> pure (Just actualEvts)
          Right _ -> do
            touchMutableByteArray x
            unlockEvent signal activeReadersVar
            pure (Just actualEvts)

documentSendException :: ByteArray -> SendException 'Uninterruptible -> IO ()
documentSendException descr = \case
  SendShutdown -> printPrefixed descr
    "[warn] Client closed connection while server was sending message sizes.\n"
  SendReset -> printPrefixed descr
    "[warn] Client reset connection while server was sending message sizes.\n"

printPrefixed :: ByteArray -> ByteString -> IO ()
printPrefixed descr msg = BC.putStr $ BC.concat
  [ BU.toByteString descr
  , msg
  ]

typed :: MutableByteArray RealWorld -> MutablePrimArray RealWorld a
typed (MutableByteArray x) = MutablePrimArray x

untype :: MutablePrimArray s a -> MutableByteArray s
untype (MutablePrimArray x) = MutableByteArray x

-- Helper function for converting offsets to sizes. Also
-- computes the total size as an Int.
copyOffsets :: (Num a, Prim a)
  => Rotera -> MutablePrimArray RealWorld a -> Int -> Int -> Int -> IO Int
{-# inline copyOffsets #-}
copyOffsets r@Rotera{maximumEvents,maxEventBytes} lenBuf actualEventId lastEventSucc ix0 = do
  offPrev0 <- PM.readOffPtr resolution (mod actualEventId maximumEvents)
  goA 0 ix0 offPrev0 (actualEventId + 1)
  where
  resolution = roteraResolutionTable r
  goA !acc !ix !offPrev !evtId = if evtId <= lastEventSucc
    then do
      off <- PM.readOffPtr resolution (mod evtId maximumEvents)
      let !rawDelta = off - offPrev
      let !sz = if rawDelta >= 0 then rawDelta else maxEventBytes + rawDelta
      PM.writePrimArray lenBuf ix (fromIntegral sz)
      goA (sz + acc) (ix + 1) off (evtId + 1)
    else pure acc :: IO Int

readMany ::
     Rotera
  -> Int -- ^ Starting message ID
  -> Int -- ^ Maximum number of events
  -> IO (Int,UnliftedArray ByteArray)
readMany r@Rotera{base,maxEventBytes,activeReadersVar} reqId maxEvts = do
  nonblockingLockEvent r reqId maxEvts >>= \case
    NonblockingResultNothing latestEventId -> pure (latestEventId,mempty)
    NonblockingResultSomething actualEventId actualEvts signal -> do
      let lastEventSucc = actualEventId + actualEvts
      lenBuf <- PM.newPrimArray maxEvts
      _ <- copyOffsets r lenBuf actualEventId lastEventSucc 0
      res <- PM.unsafeNewUnliftedArray maxEvts
      (off,offNext) <- calculateOffsets r actualEventId actualEvts
      (!addr,!x) <- mergePossibleSplit base (untype lenBuf) maxEventBytes off offNext
      let goB !ix !boff = if ix < actualEvts
            then do
              len <- PM.readPrimArray lenBuf ix
              debug $ "readMany: read length is " ++ show len
              buf <- PM.newByteArray len
              PM.copyAddrToByteArray buf 0 (PM.plusAddr addr boff) len
              buf' <- PM.unsafeFreezeByteArray buf
              PM.writeUnliftedArray res ix buf'
              goB (ix + 1) (boff + len)
            else pure ()
      goB 0 0
      touchMutableByteArray x
      unlockEvent signal activeReadersVar
      res' <- if actualEvts < maxEvts
        then do
          y <- PM.unsafeFreezeUnliftedArray res
          PM.unsafeFreezeUnliftedArray =<< PM.thawUnliftedArray y 0 actualEvts
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
read r@Rotera{base,maxEventBytes,maximumEvents,activeReadersVar} !requestedEventId = do
  debug $ "read: maximum events is " ++ show maximumEvents
  nonblockingLockEvent r requestedEventId 1 >>= \case
    NonblockingResultSomething actualEventId actualEvts tvar -> if actualEvts == 1
      then do
        debug $ "read: actualEventId is " ++ show actualEventId
        (off,offNext) <- calculateOffsets r actualEventId 1
        (dst,len) <- if offNext >= off
          then do
            let (len,addrA) = computeSingleBuffer base off offNext
            dst <- PM.newPinnedByteArray len
            PM.copyAddrToByteArray dst 0 addrA len
            pure (dst,len)
          else do
            let (len,lenA,lenB,addrA,addrB) = computeSplitBuffers
                  base maxEventBytes off offNext
            dst <- PM.newPinnedByteArray len
            PM.copyAddrToByteArray dst 0 addrA lenA
            PM.copyAddrToByteArray dst lenA addrB lenB
            pure (dst,len)
        unlockEvent tvar activeReadersVar
        let !res = BSI.PS (FP.ForeignPtr (unPtr (PM.mutableByteArrayContents dst)) (FP.PlainPtr (unMutableByteArray dst))) 0 len
        pure (Just (actualEventId,res))
      else fail "Rotera.Nonblocking.read: should not happen"
    NonblockingResultNothing _ -> pure Nothing

calculateOffsets :: Rotera -> Int -> Int -> IO (Int,Int)
calculateOffsets Rotera{base,maxEventBytes,maximumEvents} eventId maxEvts = do
  let modEventId = (mod eventId maximumEvents)
  off <- PM.readOffAddr
    (PM.plusAddr base (4096 + maxEventBytes))
    modEventId
  let modEventIdSucc = (mod (eventId + maxEvts) maximumEvents)
  offNext <- PM.readOffAddr
    (PM.plusAddr base (4096 + maxEventBytes))
    modEventIdSucc
  pure (off,offNext)

-- The caller must touch the returned mutable byte array
-- after using the address.
mergePossibleSplit ::
     Addr -- base address
  -> MutableByteArray RealWorld -- returned if nothing new is allocated
  -> Int -- max event bytes
  -> Int -- offset
  -> Int -- offset next
  -> IO (Addr,MutableByteArray RealWorld)
mergePossibleSplit base def maxEventBytes off offNext = if offNext >= off
  then do
    let addrA = (PM.plusAddr base (4096 + off))
    pure (addrA,def)
  else do
    let len = maxEventBytes + (offNext - off)
        lenA = (maxEventBytes - off)
        lenB = offNext
        addrA = (PM.plusAddr base (4096 + off))
        addrB = (PM.plusAddr base 4096)
    dst <- PM.newPinnedByteArray len
    PM.copyAddrToByteArray dst 0 addrA lenA
    PM.copyAddrToByteArray dst lenA addrB lenB
    pure (ptrToAddr (PM.mutableByteArrayContents dst),dst)

computeSingleBuffer ::
     Addr
  -> Int -- offset
  -> Int -- offset next
  -> (Int,Addr)
computeSingleBuffer base off offNext =
  let len = offNext - off
      addrA = (PM.plusAddr base (4096 + off))
   in (len,addrA)

computeSplitBuffers ::
     Addr -- base address
  -> Int -- max event bytes
  -> Int -- offset
  -> Int -- offset next
  -> (Int,Int,Int,Addr,Addr)
computeSplitBuffers base maxEventBytes off offNext =
  let len = maxEventBytes + (offNext - off)
      lenA = (maxEventBytes - off)
      lenB = offNext
      addrA = (PM.plusAddr base (4096 + off))
      addrB = (PM.plusAddr base 4096)
   in (len,lenA,lenB,addrA,addrB)

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

roteraResolutionTable :: Rotera -> Ptr Int
roteraResolutionTable Rotera{base,maxEventBytes} =
  let !(PM.Addr a) = PM.plusAddr base (4096 + maxEventBytes) in Ptr a

unlockEvent :: TVar Bool -> TVar (SmallArray ReadTicket) -> IO ()
unlockEvent tv activeReadersVar = STM.atomically
  (STM.modifyTVar' activeReadersVar (\x -> removeReadTicket x tv))
