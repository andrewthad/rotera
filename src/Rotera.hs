{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}

module Rotera
  ( Rotera
  , Settings(..)
  , push
  , read
  , new
  ) where

import Prelude hiding (read)

import Control.Monad.ST (runST)
import Data.Primitive (Addr,MutableByteArray,ByteArray)
import GHC.Exts (Addr#,MutableByteArray#,Ptr(..),RealWorld,unsafeCoerce#)
import Data.ByteString (ByteString)
import Control.Monad (when)

import qualified Foreign.ForeignPtr as FP
import qualified GHC.ForeignPtr as FP
import qualified System.IO.MMap as MM
import qualified Data.Primitive as PM
import qualified Data.Primitive.Ptr as PM
import qualified Data.Primitive.MVar as PM
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString.Internal as BSI
import qualified System.Directory as DIR

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
-- accomodate new events. As this point, a transaction occurs whose
-- only effect is to scoot right-hand bar of the dead zone further to
-- the right. The must be the only thing that happens in the transaction.
-- Otherwise, an untimely crash would corrupt the file.


-- | A persisted rotating queue that removes the oldest entries as new
--   entries come in.
data Rotera = Rotera
  !Addr
  -- Address of the memory-mapped file used for persistence. This
  -- is the address of the header page that preceeds the data section
  -- of the file.
  !Int
  -- Maximum number of bytes used to preserve logs. By adding this
  -- to the data section address, one can compute the address of the
  -- resolution table.
  !Int
  -- Maximum number of events persisted. This determines the size of
  -- the resolution table.
  !Int
  -- Dead zone size. This is number of events that exist as a buffer
  -- between the oldest event and the newest event. The dead zone
  -- helps prevent contention.
  !(PM.MVar RealWorld (PM.MutablePrimArray RealWorld Int))
  -- This is the reader and writer lock. It contains an array of all event numbers
  -- in use. The zero-index element is special and communicates the oldest active
  -- event number. Readers use this number to determine what they are allowed to
  -- read. The writer updates this number every time it begins acquiring this
  -- lock. That means that the number is incremented before the actual deletions
  -- happen. So, it is possible that the lowest event number in the remainder of
  -- the array is lower than the oldest active id. That is fine since the writer
  -- will keep reacquiring the lock until the old reader has dropped out.
  -- The preemptive bump of the oldest active event number means that new
  -- readers can join while the writer is waiting and they will not block
  -- the writer. The writer thread uses the minimal element in this array
  -- to decide whether or not it can safely delete old events. Both
  -- the writer thread and the reader threads hold this lock for
  -- very brief periods of time. However, they acquire it differently.
  -- The readers acquires it both before reading events and after reading
  -- events. The writer acquires it every time it tries to push. If it
  -- detects that there is a reader of old events, it will repeatedly
  -- attempt to reacquire it every time a reader drops out. Eventually,
  -- the reader of old events must leave.
  !(PM.MVar RealWorld ())
  -- This is used to signal the writer thread that it should try again.
  -- This only matters when a reader is reading very old events that
  -- the writer is waiting to overwrite. To send a signal, this library
  -- calls take immidiately followed by put. This means that sending a
  -- signal can block. It blocks if another reader is simultaneously
  -- sending a signal or if the writer is already checking to see if
  -- it is safe to delete old events.

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
  , settingsPath :: FilePath
    -- ^ Path to the file where logs are preserved.
  }

new :: Settings -> IO Rotera
new (Settings maxEventBytes0 maximumEvents0 deadZoneEvents0 path) = do
  let maxEventBytes = max (4096 * 16) (div maxEventBytes0 4096 * 4096)
      maximumEvents = max 1024 (div maximumEvents0 1024 * 1024)
      deadZoneEvents = min (max 2 deadZoneEvents0) (div maximumEvents 2)
      expected = fromIntegral (4096 + maxEventBytes + (maximumEvents * PM.sizeOf (undefined :: Addr)))
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
      reattemptWrite <- PM.newEmptyMVar
      m <- PM.newPrimArray 1
      PM.writePrimArray m 0 lowestEvent
      readWrite <- PM.newMVar m
      pure (Rotera base maxEventBytes maximumEvents deadZoneEvents readWrite reattemptWrite)
    else fail ("Rotera.new: expected size " ++ show expected ++ " but got size " ++ show actual)

magicByteString :: ByteString
magicByteString =
  BSI.PS (FP.ForeignPtr (unAddr (PM.byteArrayContents y)) (FP.PlainPtr (veryUnsafeThaw y))) 0 (PM.sizeOf (undefined :: Word))
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
debug = putStrLn

-- Ensure that the dead zone is large enough to handle what is going
-- to be written.
accomodate ::
     Rotera
  -> Int -- number of events
  -> Int -- total size of events 
  -> IO (Int,Int) -- next available event id and the next event data offset
accomodate !r@(Rotera base maxEventBytes maximumEvents deadZoneEvents readWrite reattemptWrite) events bytes = do
  debug "beginning accomodation"
  lowestEvent <- PM.readOffAddr base 1
  nextEvent <- PM.readOffAddr base 2
  let table = roteraResolutionTable r
  lowestEventOffset <- PM.readOffPtr table (mod lowestEvent maximumEvents)
  nextEventOffset <- PM.readOffPtr table (mod nextEvent maximumEvents)
  -- let nextEventAddr = PM.plusAddr base (nextEventOffset + 4096)
  let eventCount = nextEvent - lowestEvent
  let deltaAddr0 = lowestEventOffset - nextEventOffset
  let deltaAddr1 = if deltaAddr0 >= 0 then deltaAddr0 else maxEventBytes - deltaAddr0
  if lowestEvent == 0 && nextEvent == 0
    then do
      debug "very first event"
      pure (0,0)
    else if events < (maximumEvents - eventCount) && bytes <= deltaAddr1
      then do
        debug "enough space in buffer"
        pure (nextEvent,nextEventOffset)
      else do
        debug ("adding more space to buffer: [lowest event=" ++ show lowestEvent ++ "][next event=" ++ show nextEvent ++ "]")
        newLowestEvent <- accomodateLoop table (lowestEvent + deadZoneEvents) bytes maxEventBytes maximumEvents nextEventOffset
        waitForReads newLowestEvent base readWrite reattemptWrite
        PM.writeOffAddr base 1 newLowestEvent
        MM.mmapSynchronize (addrToPtr base) 4096
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

-- After this runs, all locks have been released by this thread. Additionally,
-- we are guaranteed that no future reads will read anything below the given
-- event id.
waitForReads ::
     Int -- minimum event id
  -> Addr -- base address
  -> PM.MVar RealWorld (PM.MutablePrimArray RealWorld Int) -- reader writer lock
  -> PM.MVar RealWorld () -- writer signal
  -> IO ()
waitForReads expectedLowestEvent base readWrite reattemptWrite = do
  _ <- PM.tryTakeMVar reattemptWrite
  m <- PM.takeMVar readWrite
  lowestEvent <- PM.readOffAddr base 1
  PM.writePrimArray m 0 lowestEvent
  actualLowestEvent <- minimumLoop m maxBound 1 =<< PM.getSizeofMutablePrimArray m
  PM.putMVar readWrite m
  if actualLowestEvent >= expectedLowestEvent
    then pure ()
    else waitForReadsLoop expectedLowestEvent readWrite reattemptWrite

-- Inform the readers, using the read-write lock, that it is now
-- forbidden to read an event whose id is below expectedLowestEvent.
-- Then, wait until any reader that had been reading earlier events
-- is finished. Once all of them have finished, we can be confident
-- that no reader will ever read an event below this id again.
waitForReadsLoop ::
     Int -- minimum event id
  -> PM.MVar RealWorld (PM.MutablePrimArray RealWorld Int) -- reader writer lock
  -> PM.MVar RealWorld () -- writer signal
  -> IO ()
waitForReadsLoop expectedLowestEvent readWrite reattemptWrite = do
  PM.takeMVar reattemptWrite
  m <- PM.takeMVar readWrite
  actualLowestEvent <- minimumLoop m maxBound 1 =<< PM.getSizeofMutablePrimArray m
  PM.putMVar readWrite m
  if actualLowestEvent >= expectedLowestEvent
    then pure ()
    else waitForReadsLoop expectedLowestEvent readWrite reattemptWrite

-- Find the smallest event id in the array.
minimumLoop :: PM.MutablePrimArray RealWorld Int -> Int -> Int -> Int -> IO Int
minimumLoop !m !acc !ix !sz = if ix < sz
  then do
    x <- PM.readPrimArray m ix
    minimumLoop m (min acc x) (ix + 1) sz
  else pure acc

-- If all of the read slots are taken, this creates a copy of the array
-- with one extra slot at the end. We use the max bound of Int to mean
-- that the slot is empty.
assignEventIdLoop :: PM.MutablePrimArray RealWorld Int -> Int -> Int -> Int -> IO (PM.MutablePrimArray RealWorld Int)
assignEventIdLoop !m !eventId !ix !sz = if ix < sz
  then do
    oldEventId <- PM.readPrimArray m ix
    if oldEventId == maxBound
      then do
        PM.writePrimArray m ix eventId
        pure m
      else assignEventIdLoop m eventId (ix + 1) sz
  else do
    n <- PM.newPrimArray (sz + 1)
    PM.copyMutablePrimArray n 0 m 0 sz
    PM.writePrimArray n sz eventId
    pure n

-- Remove an event id from the array.
removeEventIdLoop :: PM.MutablePrimArray RealWorld Int -> Int -> Int -> Int -> IO ()
removeEventIdLoop !m !eventId !ix !sz = if ix < sz
  then do
    oldEventId <- PM.readPrimArray m ix
    if oldEventId == eventId
      then PM.writePrimArray m ix maxBound
      else removeEventIdLoop m eventId (ix + 1) sz
  else fail "removeEventIdLoop did not find the identifier"

-- | Push a single event onto the queue. Its persistence is guaranteed
--   until 'flush' is called. This function is not thread-safe. It should
--   always be called from the same writer thread.
push :: Rotera -> ByteString -> IO ()
push r@(Rotera base maxEventBytes maximumEvents deadZoneEvents _ _) !bs@(BSI.PS fp _ _) = do
  let len = B.length bs
  -- Silently discard events that could lead to a situation where the dead zone
  -- is bigger than the whole queue. Maybe we should fail or return an indicator
  -- of this failure instead.
  if len < div maxEventBytes (deadZoneEvents + 2)
    then do
      (eventId, off) <- accomodate r 1 len
      FP.withForeignPtr fp $ \(Ptr p) -> do
        let src = PM.Addr p
        if off + len <= maxEventBytes
          then PM.copyAddr (PM.plusAddr base (4096 + off)) src len
          else do
            let firstFragmentSize = maxEventBytes - off
            PM.copyAddr (PM.plusAddr base (4096 + off)) src firstFragmentSize
            PM.copyAddr (PM.plusAddr base 4096) (PM.plusAddr src firstFragmentSize) (len - firstFragmentSize)
      PM.writeOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod eventId maximumEvents) off
      PM.writeOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod (eventId + 1) maximumEvents) (off + len)
      MM.mmapSynchronize (addrToPtr base) (fromIntegral (4096 + maxEventBytes + (maximumEvents * PM.sizeOf (undefined :: Addr))))
      PM.writeOffAddr base 2 (eventId + 1)
      MM.mmapSynchronize (addrToPtr base) 4096
    else pure ()
  
addrToPtr :: Addr -> Ptr a
addrToPtr (PM.Addr x) = Ptr x

-- Blocks until all pushed events have been written to disk. This function
-- is not thread-safe. It should always be called from the same writer thread.
-- flush :: Rotera -> IO ()
-- flush rotera@(Rotera base eventSectionSize maximumEvents) = msync base (roteraFileSize rotera)
  
roteraResolutionTable :: Rotera -> Ptr Int
roteraResolutionTable (Rotera base eventSectionSize _ _ _ _) =
  let !(PM.Addr a) = PM.plusAddr base (4096 + eventSectionSize) in Ptr a

-- | Read a event from the queue. If the event identifier refers to an
--   event that has already been purged, the oldest persisted event is returned
--   along with its identifier. If the event identifier refers to an event
--   that has not yet happened, the newest persisted event is returned along
--   with its identifier. This function is thread-safe.
read ::
     Rotera -- ^ rotating queue
  -> Int -- ^ event identifier
  -> IO (Int,ByteString)
read (Rotera base maxEventBytes maximumEvents _ readWrite reattemptWrite) !requestedEventId = do
  m <- PM.takeMVar readWrite
  nextEvent <- PM.readOffAddr base 2
  lowestEvent <- PM.readPrimArray m 0
  if nextEvent /= lowestEvent
    then do
      let actualEventId = min (nextEvent - 1) (max requestedEventId lowestEvent)
      n <- assignEventIdLoop m actualEventId 1 =<< PM.getSizeofMutablePrimArray m
      PM.putMVar readWrite n
      off <- PM.readOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod actualEventId maximumEvents)
      offNext <- PM.readOffAddr (PM.plusAddr base (4096 + maxEventBytes)) (mod (actualEventId + 1) maximumEvents)
      let len = offNext - off
      dst <- PM.newPinnedByteArray len
      PM.copyAddrToByteArray dst 0 (PM.plusAddr base (4096 + off)) len
      m' <- PM.takeMVar readWrite
      removeEventIdLoop m' actualEventId 1 =<< PM.getSizeofMutablePrimArray m'
      PM.putMVar readWrite m'
      _ <- PM.tryPutMVar reattemptWrite ()
      pure (actualEventId,BSI.PS (FP.ForeignPtr (unAddr (PM.mutableByteArrayContents dst)) (FP.PlainPtr (unMutableByteArray dst))) 0 len)
    else do
      PM.putMVar readWrite m
      pure (0,B.empty)

unMutableByteArray :: MutableByteArray s -> MutableByteArray# s
unMutableByteArray (PM.MutableByteArray x) = x

unAddr :: Addr -> Addr#
unAddr (PM.Addr x) = x

