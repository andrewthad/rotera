{-# language BangPatterns #-}

module Rotera.Unsafe
  ( Rotera(..)
  , Discourse(..)
    -- * Internal
  , nonblockingLockEvent
  , blockingLockEvent
  , unlockEvent
  ) where

import Control.Monad.ST (ST,runST)
import Control.Concurrent.STM (TVar)
import Data.Primitive.Addr (Addr)
import Data.Primitive (SmallArray,PrimArray,MutablePrimArray)
import GHC.Exts (RealWorld)

import qualified Control.Concurrent.STM as STM
import qualified Data.Primitive as PM

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
  !(MutablePrimArray RealWorld Int)
  -- A singleton array holding the staged next event id.
  !(TVar Discourse)
  -- This is the reader and writer lock. It contains an array of all event numbers
  -- in use. The writer updates the minimal event number every time before it
  -- attempts to acquire the lock. That means that the number
  -- is incremented before the actual deletions
  -- happen. So, it is possible that the lowest event number in
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
  -- !(PM.MVar RealWorld ())
  -- This is used to signal the writer thread that it should try again.
  -- This only matters when a reader is reading very old events that
  -- the writer is waiting to overwrite. To send a signal, this library
  -- calls take immidiately followed by put. This means that sending a
  -- signal can block. It blocks if another reader is simultaneously
  -- sending a signal or if the writer is already checking to see if
  -- it is safe to delete old events.
  -- !(TVar Int)
  -- The most recently committed value for next event id
  -- !(TVar Int)
  -- The oldest available id


-- TODO: split discource into two data types. One will have the
-- arrays and the other will have the metadata. The reason is
-- that the metadata should only ever get written to by the
-- writer thread. This will prevent unneeded wakeups.
data Discourse = Discourse
  !(PrimArray Int) -- event ids in use by reader threads, has same length as signals array
  !(SmallArray (TVar Bool)) -- used to signal that readers should stop performing blocking io
  !Int -- most recent committed id plus one, committed
  !Int -- oldest available id, both staged and committed

-- Remove an event id from the array. This is called when a reader
-- is shutting down.
removeEvent ::
     MutablePrimArray s Int
  -> PrimArray Int
  -> Int -- event id to remove, should be present in array
  -> ST s (Int,PrimArray Int)
removeEvent !m !old !eventId = go 0
  where
  !sz = PM.sizeofPrimArray old
  go !ix = if ix < sz
    then do
      let oldEventId = PM.indexPrimArray old ix
      if oldEventId == eventId
        then do
          PM.writePrimArray m ix maxBound
          PM.copyPrimArray m (ix + 1) old (ix + 1) (sz - (ix + 1))
          m' <- PM.unsafeFreezePrimArray m
          pure (ix,m')
        else go (ix + 1)
    else error "removeEvent did not find the identifier"

-- If all of the read slots are taken, this creates a copy of the array
-- with one extra slot at the end. We use the max bound of Int to mean
-- that the slot is empty. The sz argument should be equal to the size
-- of the old event id array. The new event id array should have a size
-- one greater than this. It will often be resized by this function.
assignEvent ::
     MutablePrimArray s Int
  -> SmallArray (TVar Bool)
  -> PrimArray Int
  -> TVar Bool -- Having this argument here is foolish, but it is convenient
  -> Int -- event identifier
  -> ST s (PrimArray Int,SmallArray (TVar Bool),TVar Bool)
assignEvent !new !stopBlock !old !extraStop !eventId = go 0
  where
  !sz = PM.sizeofSmallArray stopBlock
  go !ix = if ix < sz
    then do
      let oldEventId = PM.indexPrimArray old ix
      if oldEventId == maxBound
        then do
          PM.writePrimArray new ix eventId
          PM.copyPrimArray new (ix + 1) old (ix + 1) (sz - (ix + 1))
          PM.shrinkMutablePrimArray new sz
          new' <- PM.unsafeFreezePrimArray new
          let !signal = PM.indexSmallArray stopBlock ix
          pure (new',stopBlock,signal)
        else do
          PM.writePrimArray new ix oldEventId
          go (ix + 1)
    else do
      PM.writePrimArray new sz eventId
      new' <- PM.unsafeFreezePrimArray new
      newStop <- PM.newSmallArray (sz + 1) extraStop
      PM.copySmallArray newStop 0 stopBlock 0 sz
      newStop' <- PM.unsafeFreezeSmallArray newStop
      pure (new',newStop',extraStop)

nonblockingLockEvent :: TVar Discourse -> Int -> Int -> IO (Int,Int,TVar Bool)
nonblockingLockEvent !discourseVar !requestedEventId !reqEvts = do
  -- TODO: stop allocating this tvar here
  extraStop <- STM.newTVarIO False
  STM.atomically $ do
    Discourse inUse stopBlock nextEvent lowestEvent <- STM.readTVar discourseVar
    let actualEvts = min reqEvts (nextEvent - requestedEventId)
    if actualEvts > 0
      then do
        let actualEventId = max requestedEventId lowestEvent
            (newInUse,newStopBlock,signal) = runST $ do
              let inUseSz = PM.sizeofPrimArray inUse
              buf <- PM.newPrimArray (inUseSz + 1)
              assignEvent buf stopBlock inUse extraStop actualEventId
        STM.writeTVar discourseVar $! Discourse newInUse newStopBlock nextEvent lowestEvent
        pure (actualEventId,actualEvts,signal)
      else pure (0,0,extraStop)

blockingLockEvent :: TVar Discourse -> Int -> IO Int
blockingLockEvent !discourseVar !requestedEventId = do
  -- TODO: stop allocating this tvar here
  extraStop <- STM.newTVarIO False
  STM.atomically $ do
    Discourse inUse stopBlock nextEvent lowestEvent <- STM.readTVar discourseVar
    if requestedEventId < nextEvent
      then do
        let actualEventId = max requestedEventId lowestEvent
            (newInUse,newStopBlock,_) = runST $ do
              let inUseSz = PM.sizeofPrimArray inUse
              buf <- PM.newPrimArray (inUseSz + 1)
              assignEvent buf stopBlock inUse extraStop actualEventId
        STM.writeTVar discourseVar $! Discourse newInUse newStopBlock nextEvent lowestEvent
        pure actualEventId
      else STM.retry

unlockEvent :: TVar Discourse -> Int -> IO ()
unlockEvent !discourseVar !eventId = STM.atomically $ do
  Discourse inUse stopBlock nextEvent lowestEvent <- STM.readTVar discourseVar
  let (ix,newInUse) = runST $ do
        let sz = PM.sizeofPrimArray inUse
        newInUse' <- PM.newPrimArray sz
        removeEvent newInUse' inUse eventId
  STM.writeTVar (PM.indexSmallArray stopBlock ix) False
  STM.writeTVar discourseVar $! Discourse newInUse stopBlock nextEvent lowestEvent
