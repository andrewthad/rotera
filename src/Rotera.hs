module Rotera
  ( Rotera
  , Settings(..)
  , push
  , new
  ) where

import qualified System.IO.MMap as MM
import qualified Data.Primitive as PM

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
    --   to be a multiple of 4096. If it is not, it will be rounded up to
    --   the nearest multiple.
  , settingsEntries :: !Int
    -- ^ Maximum number of logs preserved.
  , settingsPath :: FilePath
    -- ^ Path to the file where logs are preserved.
  }

data Fingerprints
  = FingerprintsCons {-# UNPACK #-} !Fingerprint
  | FingerprintsNil

new :: Settings -> IO Rotera
new

-- Ensure that the dead zone is large enough to handle what is going
-- to be written.
accomodate ::
     Rotera
  -> Int -- number of events
  -> Int -- total size of events 
  -> IO (Int,Int) -- next available event id and the next event data offset
accomodate !r@(Rotera base maxEventBytes maximumEvents deadZoneEvents readWrite reattemptWrite) events bytes = do
  lowestEvent <- PM.readOffAddr base 1
  nextEvent <- PM.readOffAddr base 2
  let table = roteraResolutionTable r
  lowestEventAddr <- fmap (\off -> PM.plusAddr base (off + 4096)) $ PM.readOffPtr table (mod lowestEvent maximumEvents)
  nextEventOffset <- PM.readOffPtr table (mod nextEvent maximumEvents)
  let nextEventAddr = PM.plusAddr base (nextEventOffset + 4096)
  let eventCount = nextEvent - lowestEvent
  let deltaAddr0 = PM.minusAddr nextEventAddr lowestEventAddr
  let deltaAddr1 = if deltaAddr0 >= 0 then deltaAddr0 else maxEventBytes - deltaAddr0
  if events < (maximumEvents - eventCount) && bytes <= deltaAddr1
    then do pure (nextEvent,nextEventOffset)
    else do
      newLowestEvent <- accomodateLoop table (lowestEvent + deadZoneEvents) bytes maxEventBytes maximumEvents nextEventAddr
      waitForReads newLowestEvent readWrite reattemptWrite
      PM.writeOffAddr base 1 newLowestEvent
      msync base 4096
      pure (nextEvent,nextEventOffset)

-- Returns the event id that needs to become the new minimum.
accomodateLoop :: Ptr Addr -> Int -> Int -> Int -> Int -> Addr -> IO Int
accomodateLoop resolution eventId bytes maxEventBytes maximumEvents addr0 = do
  addr <- PM.readPtrOff resolution (mod eventId maximumEvents)
  let deltaAddr0 = PM.minusAddr addr addr0
  let deltaAddr1 = if deltaAddr0 >= 0 then deltaAddr0 else maxEventBytes - deltaAddr0
  if bytes <= deltaAddr1
    then pure (eventId + 1)
    else accomodateLoop resolution (eventId + 1) bytes maxEventBytes addr0

-- After this runs, all locks have been released by this thread. Additionally,
-- we are guaranteed that no future reads will read anything below the given
-- event id.
waitForReads ::
     Int -- minimum event id
  -> PM.MVar RealWorld (PM.MutablePrimArray RealWorld Int) -- reader writer lock
  -> PM.MVar RealWorld () -- writer signal
  -> IO ()
waitForReads expectedLowestEvent readWrite reattemptWrite = do
  _ <- PM.tryTakeMVar reattemptWrite
  m <- PM.takeMVar readWrite
  PM.writePrimArray m 0 lowestEvent
  actualLowestEvent <- minimumLoop m maxBound 1 =<< PM.getSizeofPrimArray m
  PM.putMVar readWrite m
  if actualLowestEvent >= expectedLowestEvent
    then pure ()
    else waitForReadsLoop expectedLowestEvent readWrite reattemptWrite

waitForReadsLoop ::
     Int -- minimum event id
  -> PM.MVar RealWorld (PM.MutablePrimArray RealWorld Int) -- reader writer lock
  -> PM.MVar RealWorld () -- writer signal
  -> IO ()
waitForReadsLoop expectedLowestEvent readWrite reattemptWrite = do
  PM.takeMVar reattemptWrite
  m <- PM.takeMVar readWrite
  actualLowestEvent <- minimumLoop m maxBound 1 =<< PM.getSizeofPrimArray m
  PM.putMVar readWrite m
  if actualLowestEvent >= expectedLowestEvent
    then pure ()
    else waitForReadsLoop expectedLowestEvent readWrite reattemptWrite

minimumLoop :: PM.MutablePrimArray RealWorld Int -> Int -> Int -> Int -> IO Int
minimumLoop !m !acc !ix !sz = if ix < sz
  then do
    x <- M.readPrimArray m ix
    minimumLoop m (min acc x) (ix + 1) sz
  else acc
  

-- | Push a single event onto the queue. Its persistence is guaranteed
--   until 'flush' is called. This function is not thread-safe. It should
--   always be called from the same writer thread.
push :: Rotera -> ByteString -> IO ()
push r@(Rotera base maxEventBytes maximumEvents deadZoneEvents readWrite reattemptWrite) !bs@(BSI.PS fp _ _) = do
  let len = B.length bs
  (eventId, off) <- accomodate r 1 len
  withForeignPtr fp $ \(Ptr p) -> let src = Addr p in if off + len <= maxEventBytes
    then do
      PM.copyAddr (PM.plusAddr base (4096 + off)) p len
      PM.copyAddr (PM.plusAddr base (4096 + maxEventBytes + mod eventId maximumEvents)) p len
      PM.writeOffAddr base 2 (eventId + 1)
    else _
  
-- Blocks until all pushed events have been written to disk. This function
-- is not thread-safe. It should always be called from the same writer thread.
flush :: Rotera -> IO ()
flush rotera@(Rotera base eventSectionSize maximumEvents) = msync base (roteraFileSize rotera)
  
roteraResolutionTable :: Rotera -> Ptr Int
roteraResolutionTable (Rotera base eventSectionSize _ _ _ _) =
  let Addr a = plusAddr base (4096 + eventSectionSize) in Ptr a

roteraFileSize :: Rotera -> Int
roteraFileSize (Rotera _ eventSectionSize maximumEvents _ _ _) =
  4096 + eventSectionSize + (maximumEvents * PM.sizeOf (undefined :: Addr))

-- | Read a event from the queue. If the event identifier refers to an
--   event that has already been purged, the oldest persisted event is returned
--   along with its identifier. If the event identifier refers to an event
--   that has not yet happened, the newest persisted event is returned along
--   with its identifier. This function is thread-safe.
read ::
     Rotera -- ^ rotating queue
  -> Int -- ^ event identifier
  -> IO (Int,ByteString)

msync :: Addr -> Int -> IO ()
msync = error "msync: write me"


