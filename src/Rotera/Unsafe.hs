{-# language BangPatterns #-}
{-# language NamedFieldPuns #-}
{-# language RankNTypes #-}

module Rotera.Unsafe
  ( Rotera(..)
  , Discourse(..)
  , ReadTicket(..)
  , NonblockingResult(..)
  , EventRange(..)
  , WriterLock(..)
    -- * Internal
  , nonblockingLockEvent
  , blockingLockEvent
  , removeReadTicket
  ) where

import Control.Monad.ST (ST,runST)
import Control.Concurrent.STM (TVar)
import Data.Primitive.Addr (Addr)
import Data.Primitive (SmallArray,PrimArray,MutablePrimArray)
import Data.Primitive (SmallMutableArray)
import GHC.Exts (RealWorld)

import qualified Control.Concurrent.STM as STM
import qualified Data.Primitive as PM
import qualified Data.Primitive.PrimArray.Atomic as PM

-- | A persisted rotating queue that removes the oldest entries as new
--   entries come in.
data Rotera = Rotera
  { base :: !Addr
    -- Address of the memory-mapped file used for persistence. This
    -- is the address of the header page that preceeds the data section
    -- of the file.
  , maxEventBytes :: !Int
    -- Maximum number of bytes used to preserve logs. By adding this
    -- to the data section address, one can compute the address of the
    -- resolution table.
  , maximumEvents :: !Int
    -- Maximum number of events persisted. This determines the size of
    -- the resolution table.
  , deadZoneEvents :: !Int
    -- Dead zone size. This is number of events that exist as a buffer
    -- between the oldest event and the newest event. The dead zone
    -- helps prevent contention.
  , stagingBuf :: !(MutablePrimArray RealWorld Int)
    -- A singleton array holding the staged next event id.
  , activeReadersVar :: !(TVar (SmallArray ReadTicket))
    -- Information about all the active readers.
  , eventRangeVar :: !(TVar EventRange)
    -- The oldest available id, and the most recently committed
    -- value for next event id.
  , writerVar :: !(TVar WriterLock)
    -- This serves two purposes. It acts as a lock that a writer
    -- takes to gain exclusive access to the queue, and it provides
    -- a way for competing writer to tell the active writer to
    -- hurry up. Hurrying means writing everything to auxiliary
    -- memory and performing a memcpy once everything has arrived.
  }

data WriterLock
  = WriterLockLocked !(TVar Bool)
  | WriterLockUnlocked

-- Oldest event id and next event id
data EventRange = EventRange !Int !Int

-- Globally, for readers, we have:
-- (TVar (SmallArray ReadTicket), TVar Int, TVar Int)
-- And each individual reader holds onto its ticket id and
-- its signal locally.
--
-- Globally, for writers, we have:
-- (TVar (Maybe (TVar Bool)))
-- And each writer holds onto its signal locally. If there
-- is a signal present in the global TVar, it means that
-- someone is writing to the queue. A competing writer can use this
-- signal to tell the other writer to hurry up.

-- TODO: split discourse into three data types. One will have the
-- arrays and the other will have the metadata. The reason is
-- that the metadata should only ever get written to by the
-- writer thread. This will prevent unneeded wakeups.
data Discourse = Discourse
  !(PrimArray Int) -- event ids in use by reader threads, has same length as signals array
  !(SmallArray (TVar Bool)) -- used to signal that readers should stop performing blocking io
  !Int -- most recent committed id plus one, committed
  !Int -- oldest available id, both staged and committed

-- Take a ticket when you are reading from the queue
data ReadTicket = ReadTicket
  !Int
  -- The lowest id in the batch being read
  !(TVar Bool)
  -- A signal a writer can use to tell this reader
  -- to stop performing blocking IO

-- Remove an event id from the array. This is called when a reader
-- is shutting down.
removeReadTicket ::
     SmallArray ReadTicket
  -> TVar Bool -- read ticket TVar
  -> SmallArray ReadTicket
removeReadTicket !old !identNeedle = runST $ do
  let sz = PM.sizeofSmallArray old
  marr <- PM.newSmallArray (sz - 1) errorThunk
  go marr (sz - 1) (sz - 2)
  where
  go :: forall s. SmallMutableArray s ReadTicket -> Int -> Int -> ST s (SmallArray ReadTicket)
  go !marr !ixSrc !ixDst = if ixDst >= 0
    then do
      let r@(ReadTicket _ ident) = PM.indexSmallArray old ixSrc
      if ident == identNeedle
        then go marr (ixSrc - 1) ixDst
        else do
          PM.writeSmallArray marr ixDst r
          go marr (ixSrc - 1) (ixDst - 1)
    else PM.unsafeFreezeSmallArray marr

errorThunk :: a
{-# NOINLINE errorThunk #-}
errorThunk = error "rotera: whomp"

snocSmallArray :: a -> SmallArray a -> SmallArray a
{-# inline snocSmallArray #-}
snocSmallArray x arr = runST $ do
  let sz = PM.sizeofSmallArray arr
  marr <- PM.newSmallArray (sz + 1) x
  PM.copySmallArray marr 0 arr 0 sz
  PM.unsafeFreezeSmallArray marr

-- If all of the read slots are taken, this creates a copy of the array
-- with one extra slot at the end. We use the max bound of Int to mean
-- that the slot is empty. The sz argument should be equal to the size
-- of the old event id array. The new event id array should have a size
-- one greater than this. It will often be resized by this function.
assignEvent ::
     MutablePrimArray s Int
  -> SmallArray (TVar Bool)
  -> PrimArray Int
  -> TVar Bool
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

-- Is there data to be read or not?
data NonblockingResult
  = NonblockingResultSomething !Int !Int !(TVar Bool)
  | NonblockingResultNothing !Int

nonblockingLockEvent ::
     Rotera
  -> Int
  -> Int
  -> IO NonblockingResult
nonblockingLockEvent Rotera{activeReadersVar,eventRangeVar} !requestedEventId !reqEvts = STM.atomically $ do
  EventRange lowestEvent nextEvent <- STM.readTVar eventRangeVar
  let (actualEvts,actualEventId) = if requestedEventId == (-1)
        then
          let y = min reqEvts (nextEvent - lowestEvent)
           in (y,nextEvent - y)
        else
          let x = max requestedEventId lowestEvent
           in (min reqEvts (nextEvent - x),x)
  if actualEvts > 0
    then do
      activeReaders <- STM.readTVar activeReadersVar
      newSignal <- STM.newTVar False
      let !newTicket = ReadTicket actualEventId newSignal
      STM.writeTVar activeReadersVar $! snocSmallArray newTicket activeReaders
      pure $! NonblockingResultSomething actualEventId actualEvts newSignal
    else pure (NonblockingResultNothing actualEventId)

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
