{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language ScopedTypeVariables #-}
{-# language NamedFieldPuns #-}

module Rotera
  ( Rotera(..)
  , Settings(..)
  , pushMany
  , push
  , new
  , open
  , commit
  ) where

import Prelude hiding (read)
import Rotera.Unsafe (Rotera(..),ReadTicket(..),EventRange(..))
import Rotera.Unsafe (WriterLock(..))

import Control.Concurrent.STM (TVar)
import Control.Monad (when)
import Control.Monad.ST (runST)
import Data.Primitive (Prim)
import Data.ByteString (ByteString)
import Data.Bytes.Types (MutableBytes(..))
import Data.Primitive (ByteArray)
import Data.Primitive (SmallArray,MutablePrimArray)
import Data.Primitive.Addr (Addr)
import GHC.Exts (Addr#,MutableByteArray#,Ptr(..),RealWorld,unsafeCoerce#)

import qualified Control.Concurrent.STM as STM
import qualified Foreign.ForeignPtr as FP
import qualified GHC.ForeignPtr as FP
import qualified System.IO.MMap as MM
import qualified Data.Primitive.Addr as PM
import qualified Data.Primitive as PM
import qualified Data.Primitive.Ptr as PM
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString.Internal as BSI
import qualified System.Directory as DIR
import qualified Data.Vector.Primitive.Mutable as PV

-- A memory mapped file is used to persist events in a circular queue. In memory,
-- this is layed out as:
--
--   +-------------------+--------------+------------------+
--   | Header Page (4KB) | Data Section | Resolution Table |
--   +-------------------+--------------+------------------+
--
-- The data section is by far the largest. Its size is given as the "size"
-- parameter in the Settings data type. The resolution table is typically
-- much smaller. Its size is (entries / word_size), where word_size is
-- determined by the platform but is typically 64. The header page is
-- structured as:
--
-- * Magic byte
-- * Lowest event identifier (right side of dead zone)
-- * Next event identifier (left side of dead zone, interpreted in future)
--
-- Envision the data section as:
--
--  226 227 228 220 221 222 223 224 225
--   |   |   |   |---|---|---|   |   |
--   +-------------------------------+
--               | dead zone |
--
-- The resolution table is a map from event identifiers to offsets in
-- the data section. Although it is logically treated as a map,
-- it is represented as an array in which the indices are interpretted
-- as the keys. It also has a dead zone in the same event number range.
--
-- As pushes happen, the dead zone shrinks, replacing old elements with
-- new ones. Visually, the left-hand bar of the dead zone moves toward
-- the right-hand one. Eventually, the dead zone becomes too small to
-- accomodate new events. At this point, a transaction occurs whose
-- only effect is to scoot right-hand bar of the dead zone further to
-- the right. The must be the only thing that happens in the transaction.
-- Otherwise, an untimely crash would corrupt the file.

data Settings = Settings
  { settingsSize :: !Int
    -- ^ Maximum number of bytes used to preserve logs. This is expected
    --   to be a multiple of 4096. If it is not, it will be rounded down to
    --   the nearest multiple.
  , settingsEntries :: !Int
    -- ^ Maximum number of logs preserved. This is expected to be a multiple
    --   of 1024. If it is not, it will be rounded down.
  , settingsExpiredEntries :: !Int
    -- ^ Number of expired entries to keep as a buffer between
    --   the newest entry and old entries.
  , settingsRotFiles :: FilePath
    -- ^ Rot file. Ends in *.rot.
  }

open :: String -> IO Rotera
open path = do
  (Ptr a, rawsize, offset, size) <- MM.mmapFilePtr path MM.ReadWrite Nothing
  let base = PM.Addr a
  header <- PM.readOffAddr base 0
  when (header /= magicHeader) (fail "Rotera.open: magic header was invalid")
  lowestEvent <- PM.readOffAddr base 1
  nextEvent <- PM.readOffAddr base 2
  maxEventBytes <- PM.readOffAddr base 3
  maximumEvents <- PM.readOffAddr base 4
  deadZoneEvents <- PM.readOffAddr base 5
  let expectedSize = fromIntegral (4096 + maxEventBytes + (maximumEvents * PM.sizeOf (undefined :: Word)))
  when (rawsize /= expectedSize) (fail ("Rotera.open: mmapped file had rawsize " ++ show rawsize ++ " instead of expected size " ++ show expectedSize))
  when (size /= expectedSize) (fail ("Rotera.open: mmapped file had size " ++ show size ++ " instead of expected size " ++ show expectedSize))
  when (offset /= 0) (fail "Rotera.open: mmapped file had non-size offset")
  stagingBuf <- PM.newPrimArray 1 :: IO (MutablePrimArray RealWorld Int)
  PM.writePrimArray stagingBuf 0 nextEvent
  activeReadersVar <- STM.newTVarIO (mempty :: SmallArray ReadTicket)
  eventRangeVar <- STM.newTVarIO $! EventRange lowestEvent nextEvent
  writerVar <- STM.newTVarIO WriterLockUnlocked
  debug ("open: maximum events = " ++ show maximumEvents)
  pure $ Rotera
    { base,maxEventBytes,maximumEvents,deadZoneEvents
    , stagingBuf,activeReadersVar,eventRangeVar
    , writerVar
    }

new :: Settings -> IO Rotera
new (Settings maxEventBytes0 maximumEvents0 deadZoneEvents0 path) = do
  let maxEventBytes = max 4096 (div maxEventBytes0 4096 * 4096)
      maximumEvents = max 1024 (div maximumEvents0 1024 * 1024)
      deadZoneEvents = min (max 2 deadZoneEvents0) (div maximumEvents 2)
      expected = fromIntegral (4096 + maxEventBytes + (maximumEvents * PM.sizeOf (undefined :: Word)))
  DIR.doesFileExist path >>= \case
    True -> pure ()
    False -> LB.writeFile path (LB.fromStrict magicByteString <> LB.replicate (fromIntegral expected - fromIntegral (PM.sizeOf (undefined :: Word))) 0)
  actual <- DIR.getFileSize path
  if fromIntegral actual == expected
    then do
      (Ptr a, rawsize, offset, size) <- MM.mmapFilePtr path MM.ReadWrite Nothing
      let base = PM.Addr a
      when (rawsize /= expected) (fail ("Rotera.new: mmapped file had rawsize " ++ show rawsize ++ " instead of expected size " ++ show expected))
      when (size /= expected) (fail ("Rotera.new: mmapped file had size " ++ show size ++ " instead of expected size " ++ show expected))
      when (offset /= 0) (fail "Rotera.new: mmapped file had non-size offset")
      header <- PM.readOffAddr base 0
      when (header /= magicHeader) (fail "Rotera.new: magic header was invalid")
      lowestEvent <- PM.readOffAddr base 1
      nextEvent <- PM.readOffAddr base 2
      PM.writeOffAddr base 3 maxEventBytes
      PM.writeOffAddr base 4 maximumEvents
      PM.writeOffAddr base 5 deadZoneEvents
      stagingBuf <- PM.newPrimArray 1 :: IO (MutablePrimArray RealWorld Int)
      PM.writePrimArray stagingBuf 0 nextEvent
      eventRangeVar <- STM.newTVarIO $! EventRange lowestEvent nextEvent
      activeReadersVar <- STM.newTVarIO (mempty :: SmallArray ReadTicket)
      writerVar <- STM.newTVarIO WriterLockUnlocked
      debug ("new: maximum events = " ++ show maximumEvents)
      pure $ Rotera
        { base,maxEventBytes,maximumEvents,deadZoneEvents
        , stagingBuf,activeReadersVar,eventRangeVar
        , writerVar
        }
    else fail ("Rotera.new: expected size " ++ show expected ++ " but got size " ++ show actual)

magicByteString :: ByteString
magicByteString =
  BSI.PS (FP.ForeignPtr (unPtr (PM.byteArrayContents y)) (FP.PlainPtr (veryUnsafeThaw y))) 0 (PM.sizeOf (undefined :: Word))
  where
  y = runST $ do
    x <- PM.newPinnedByteArray (PM.sizeOf (undefined :: Word))
    PM.writeByteArray x 0 magicHeader
    PM.unsafeFreezeByteArray x

veryUnsafeThaw :: ByteArray -> MutableByteArray# s
veryUnsafeThaw (PM.ByteArray x) = unsafeCoerce# x

magicHeader :: Word
magicHeader = 0xcb242feb29866985

debug :: String -> IO ()
debug _ = pure ()
-- debug = putStrLn

-- Ensure that the dead zone is large enough to handle what is going
-- to be written.
--
-- Precondition: The caller of this function must hold the writer lock
-- It is never relinquished while this function is running.
accomodate ::
     Rotera
  -> Int -- number of events
  -> Int -- total size of events
  -> IO (Int,Int) -- next available event id and the next event data offset
accomodate !r@Rotera{base,maxEventBytes,maximumEvents,deadZoneEvents,stagingBuf,activeReadersVar,eventRangeVar} events bytes = do
  debug "beginning accomodation"
  nextEvent <- PM.readPrimArray stagingBuf 0
  EventRange lowestEvent _ <- STM.readTVarIO eventRangeVar
  let table = roteraResolutionTable r
  lowestEventOffset <- PM.readOffPtr table (mod lowestEvent maximumEvents)
  nextEventOffset <- PM.readOffPtr table (mod nextEvent maximumEvents)
  let eventCount = nextEvent - lowestEvent
  let deltaAddr0 = lowestEventOffset - nextEventOffset
  let deltaAddr1 = if deltaAddr0 >= 0 then deltaAddr0 else maxEventBytes + deltaAddr0
  debug ("making resize decision [event data max=" ++ show maxEventBytes ++ "][lowest offset=" ++ show lowestEventOffset ++ "][next offset=" ++ show nextEventOffset ++ "][remaining bytes: " ++ show deltaAddr1 ++ "]")
  if lowestEvent == 0 && nextEvent == 0
    then do
      debug "very first event"
      pure (0,0)
    -- I believe that we must leave an extra event for padding.
    -- That's why we subtract one.
    else if events < (maximumEvents - eventCount - 1) && bytes <= deltaAddr1
      then do
        debug "enough space in buffer"
        pure (nextEvent,nextEventOffset)
      else do
        debug "adding more space to buffer"
        newLowestEvent <- accomodateLoop table
          (lowestEvent + deadZoneEvents) bytes
          maxEventBytes maximumEvents nextEventOffset
        debug ("decided new lowest event [new lowest event=" ++ show newLowestEvent ++ "]")
        PM.writeOffAddr base 1 newLowestEvent
        -- We replace the lowest allowed event identifier before
        -- signal any readers of old things to hurry up. This means
        -- that any new readers that show up after the signals are
        -- sent cannot be reading events earlier than newLowestEvent.
        replaceLowest newLowestEvent eventRangeVar
        good <- waitForReadsPhase1 newLowestEvent activeReadersVar
        when (not good) (waitForReadsPhase2 newLowestEvent activeReadersVar)
        -- TODO: calling commit here is not great. We probably want to
        -- split up commit into two functions or factor common parts of
        -- it out.
        internalCommit r
        pure (nextEvent,nextEventOffset)

-- Returns the event id that needs to become the new minimum.
accomodateLoop :: Ptr Int -> Int -> Int -> Int -> Int -> Int -> IO Int
accomodateLoop resolution eventId bytes maxEventBytes maximumEvents off0 = do
  off <- PM.readOffPtr resolution (mod eventId maximumEvents)
  let deltaAddr0 = off - off0
  let deltaAddr1 = if deltaAddr0 >= 0 then deltaAddr0 else maxEventBytes - deltaAddr0
  if bytes <= deltaAddr1
    then pure (eventId + 1)
    else accomodateLoop resolution (eventId + 1) bytes maxEventBytes maximumEvents off0

replaceLowest ::
     Int -- minimum event id
  -> TVar EventRange
  -> IO ()
replaceLowest !newLowestEvent !var = STM.atomically $ do
  EventRange _ nextEvent <- STM.readTVar var
  STM.writeTVar var $! EventRange newLowestEvent nextEvent

-- After this runs, all locks have been released by this thread. Additionally,
-- we are guaranteed that no future reads will read anything below the given
-- event id.
waitForReadsPhase1 ::
     Int -- minimum event id
  -> TVar (SmallArray ReadTicket)
  -> IO Bool
waitForReadsPhase1 !expectedLowestEvent !var = STM.atomically $ do
  tickets <- STM.readTVar var
  let go !ix good = if ix >= 0
        then do
          let (ReadTicket actualLowestEvent stopBlockVar) = PM.indexSmallArray tickets ix
          if actualLowestEvent >= expectedLowestEvent
            then go (ix - 1) good
            else do
              STM.writeTVar stopBlockVar True
              go (ix - 1) False
        else pure good
  go (PM.sizeofSmallArray tickets - 1) True

waitForReadsPhase2 ::
     Int -- minimum event id
  -> TVar (SmallArray ReadTicket)
  -> IO ()
waitForReadsPhase2 !expectedLowestEvent !var = STM.atomically $ do
  tickets <- STM.readTVar var
  let go !ix = if ix >= 0
        then do
          let ReadTicket actualLowestEvent _ = PM.indexSmallArray tickets ix
          if actualLowestEvent >= expectedLowestEvent
            then go (ix - 1)
            else STM.retry
        else pure ()
  go (PM.sizeofSmallArray tickets - 1)

-- Invariant: the length of payloads is equal to the sum of the elements
-- in sizes.
pushMany :: (Integral a, Prim a)
  => Rotera
  -> PV.MVector RealWorld a -- ^ sizes (length is total number of events)
  -> MutableBytes RealWorld -- ^ all data bytes smashed together
  -> IO ()
{-# inline pushMany #-}
pushMany r@Rotera{base,maxEventBytes,maximumEvents,deadZoneEvents,stagingBuf,writerVar} lens payloads = do
  let events = PV.length lens
  -- Perform sanity check.
  -- TODO: Stop doing this here. The server must already fold over
  -- the lengths to calculate the total.
  vecMapM_
    (\len -> if fromIntegral len > div maxEventBytes (deadZoneEvents + 2)
      then fail "pushMany: event too big"
      else pure ()
    ) lens
  when (events >= deadZoneEvents - 1) $ do
    fail $ "pushMany: too many events, max is " ++ show (deadZoneEvents - 1)
  myLock <- STM.newTVarIO False
  -- Discard the signal since there is not anything we can do
  -- to hurry up.
  _ <- acquireWriter myLock writerVar
  (eventId0, off0) <- accomodate r events plen
  if off0 + plen <= maxEventBytes
    then PM.copyMutableByteArrayToAddr (addrToPtr (PM.plusAddr base (4096 + off0))) src poff plen
    else do
      let firstFragmentSize = maxEventBytes - off0
      PM.copyMutableByteArrayToAddr (addrToPtr (PM.plusAddr base (4096 + off0))) src poff firstFragmentSize
      PM.copyMutableByteArrayToAddr (addrToPtr (PM.plusAddr base 4096)) src (firstFragmentSize + poff) (plen - firstFragmentSize)
  !_ <- vecFoldl
    ( \(eventId,off) len -> do
      let nextOff = mod (off + fromIntegral len) maxEventBytes
          nextEventId = mod (eventId + 1) maximumEvents
      PM.writeOffAddr
        (PM.plusAddr base (4096 + maxEventBytes))
        nextEventId
        nextOff
      pure (nextEventId,nextOff)
    ) (eventId0,off0) lens
  PM.writePrimArray stagingBuf 0 (eventId0 + events)
  -- Release the writer lock
  STM.atomically (STM.writeTVar writerVar WriterLockUnlocked)
  where
  MutableBytes src poff plen = payloads

-- When this finishes running, the myLock argument is in
-- a WriterLockLocked in the var.
acquireWriter :: TVar Bool -> TVar WriterLock -> IO ()
acquireWriter !myLock !var = acquireWriterLoop myLock myLock var

-- The purpose of this function is to notify the active writer
-- that we need them to hurry up, or if there is no active writer
-- acquire the writer lock.
acquireWriterLoop :: TVar Bool -> TVar Bool -> TVar WriterLock -> IO ()
acquireWriterLoop !myLock !prevSignal !var = do
  res <- STM.atomically $ STM.readTVar var >>= \case
    WriterLockUnlocked -> do
      let !w = WriterLockLocked myLock
      STM.writeTVar var w
      pure Nothing
    WriterLockLocked signal -> if signal == prevSignal
      then STM.retry
      else do
        STM.writeTVar signal True
        pure (Just signal)
  case res of
    Just signal -> acquireWriterLoop myLock signal var
    Nothing -> pure ()

vecMapM_ :: Prim a => (a -> IO b) -> PV.MVector RealWorld a -> IO ()
{-# inline vecMapM_ #-}
vecMapM_ f v = go 0 where
  go !ix = if ix < PV.length v
    then (f =<< PV.unsafeRead v ix) *> go (ix + 1)
    else pure ()

vecFoldl :: Prim a => (b -> a -> IO b) -> b -> PV.MVector RealWorld a -> IO b
vecFoldl f !b0 !v = go 0 b0 where
  go !ix !b = if ix < PV.length v
    then (f b =<< PV.unsafeRead v ix) >>= go (ix + 1)
    else pure b

addrToPtr :: Addr -> Ptr a
addrToPtr (PM.Addr x) = Ptr x

commit :: Rotera -> IO ()
commit r@Rotera{writerVar} = do
  myLock <- STM.newTVarIO False
  acquireWriter myLock writerVar
  internalCommit r
  STM.atomically (STM.writeTVar writerVar WriterLockUnlocked)

-- Blocks until all pushed events have been written to disk. This function
-- is not thread-safe. It should always be called in a context where the
-- caller has already acquired the writer lock.
internalCommit :: Rotera -> IO ()
internalCommit Rotera{base,maxEventBytes,maximumEvents,stagingBuf,eventRangeVar} = do
  MM.mmapSynchronize (addrToPtr base)
    (fromIntegral (4096 + maxEventBytes + (maximumEvents * PM.sizeOf (undefined :: Word))))
  oldNextEvent <- PM.readOffAddr base 2
  newNextEvent <- PM.readPrimArray stagingBuf 0
  debug $ "commit: old next event was " ++ show oldNextEvent ++ " and new is " ++ show newNextEvent
  PM.writeOffAddr base 2 (newNextEvent :: Int)
  MM.mmapSynchronize (addrToPtr base) 4096
  when (oldNextEvent /= newNextEvent) $ STM.atomically $ do
    EventRange lowestEvent _ <- STM.readTVar eventRangeVar
    STM.writeTVar eventRangeVar $! EventRange lowestEvent newNextEvent

-- | Push a single event onto the queue. Its persistence is not guaranteed
--   until 'commit' is called. This function is safe to use concurrently
--   with itself.
push :: Rotera -> ByteString -> IO ()
push r@Rotera{base,maxEventBytes,maximumEvents,deadZoneEvents,stagingBuf,writerVar} (BSI.PS fp poff len) = do
  -- Silently discard events that could lead to a situation where the dead zone
  -- is bigger than the whole queue. Maybe we should fail or return an indicator
  -- of this failure instead.
  if len < div maxEventBytes (deadZoneEvents + 2)
    then do
      myLock <- STM.newTVarIO False
      -- Discard the signal since there is not anything we can do
      -- to hurry up.
      acquireWriter myLock writerVar
      (eventId, off) <- accomodate r 1 len
      FP.withForeignPtr fp $ \(Ptr p) -> do
        let src = PM.Addr p
        debug ("push: copying " ++ show len ++ " bytes")
        if off + len <= maxEventBytes
          then PM.copyAddr (PM.plusAddr base (4096 + off)) (PM.plusAddr src poff) len
          else do
            let firstFragmentSize = maxEventBytes - off
            PM.copyAddr (PM.plusAddr base (4096 + off)) (PM.plusAddr src poff) firstFragmentSize
            PM.copyAddr (PM.plusAddr base 4096) (PM.plusAddr src (firstFragmentSize + poff)) (len - firstFragmentSize)
      -- TODO: I commented this line out because we definitely should not be doing
      -- two writes to the offset table. I need to think more carefully about what
      -- happens on the very first push, but I think what we have now works.
      -- PM.writeOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod eventId maximumEvents) off
      let nextOffsetPos = (mod (eventId + 1) maximumEvents)
          nextOffset = (mod (off + len) maxEventBytes) :: Int
      debug $ "push: next offset position is " ++ show nextOffsetPos ++ " and value is " ++ show nextOffset
      PM.writeOffAddr
        (PM.plusAddr base (4096 + maxEventBytes))
        nextOffsetPos
        nextOffset
      PM.writePrimArray stagingBuf 0 (eventId + 1)
      -- Release the writer lock
      STM.atomically (STM.writeTVar writerVar WriterLockUnlocked)
    else fail "push: too big"

roteraResolutionTable :: Rotera -> Ptr Int
roteraResolutionTable Rotera{base,maxEventBytes} =
  let !(PM.Addr a) = PM.plusAddr base (4096 + maxEventBytes) in Ptr a

unPtr :: Ptr a -> Addr#
unPtr (Ptr x) = x

-- insert ::
--      TVar (Maybe Node) -- root node
--   -> Node
--   -> STM ()
-- insert !root !node = readTVar root >>= \case
--   Nothing -> do
--     let !newRoot = Just node
--     STM.writeTVar root newRoot
--   Just rootNode@(Node _ rootRight _ _) -> do
--     rootRightNode@(Node rootRightLeft _ _ _) <- STM.readTVar rootRight

-- data Blockingness = Blocking | Nonblocking
--
-- -- The API I am envisioning for stream-socket communication
-- data RequestMessages
--   Uuid -- the queue that should be used
--   Blockingness -- should the server wait for more events
--   (Int64 | MostRecent) -- first identifier to start with, use zero for oldest
--   Word64 -- number of messages, maxBound is effectively infinite
--
-- data ResponseMessages = ResponseMessages (ListT IO (MessageId,Messages))
--
-- -- There is no response from the server when pushing.
-- data PushMessages = PushMessages Uuid Messages
--
-- data Commit = Commit Uuid
--
-- data Messages = Messages
--   PV.Vector Word32 -- sizes of the messages
--   Bytes -- all the messages concatenated

-- Another attempt.
-- The protocol is something like:
-- * Client sends request type and attributes (fixed size)
-- * Client sends request body (size known from request type)
-- * Server might respond depending on the request type
-- The three types of requests are:
-- * Commit Uuid (flush changes to this queue)
--   Response is an acknowledgement
-- * Push Uuid MsgCount -> followed by msg lens and then by concatenated messages
--   Server does not acknowledge this
-- * Read Uuid Blockingness (Int64 | MostRecent) Word64 -> followed by nothing
--   Server responds with messages
-- * Ping -> followed by nothing
--   Server responds
-- All server responses include a byte for graceful shutdown.
-- This tells the client that it needs to shutdown the connection.
