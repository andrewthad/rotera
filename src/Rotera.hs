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
-- * Magic byte sequence
-- * Lowest event identifier
-- * Next event identifier



-- | A persisted rotating queue that removes the oldest entries as new
--   entries come in.
data Rotera = Rotera
  !Addr -- Address of the memory-mapped file used for persistence. This
        -- is the address of the header page that preceeds the data section
        -- of the file.
  !Int  -- Maximum number of bytes used to preserve logs. By adding this
        -- to the data section address, one can compute the address of the
        -- resolution table.
  !Int  -- Maximum number of events persisted. This determines the size of
        -- the resolution table.

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

-- | Push a single event onto the queue. Its persistence is not guaranteed
--   until 'flush' is called.  This function is not thread-safe. It should
--   always be called from the same writer thread.
push :: Rotera -> ByteString -> IO ()
push (Rotera base eventSize) !bs = do
  
  
-- | Blocks until all pushed events have been written to disk. This function
--   is not thread-safe. It should always be called from the same writer thread.
flush :: Rotera -> IO ()
flush rotera@(Rotera base eventSectionSize maximumEvents) = do
  msync base (roteraFileSize rotera)
  m

roteraFileSize :: Rotera -> Int
roteraFileSize (Rotera _ eventSectionSize maximumEvents) =
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


