{-# language DuplicateRecordFields #-}
{-# language BangPatterns #-}
{-# language NamedFieldPuns #-}
{-# language ScopedTypeVariables #-}
{-# language DeriveAnyClass #-}
{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language TypeApplications #-}
{-# language StandaloneDeriving #-}
{-# language DerivingStrategies #-}

module Rotera.Client
  ( ping
  , read
  , push
  , commit
  , pull
  , stream
  , Queue(..)
  , Message(..)
  , Batch(..)
  , Status(..)
  ) where

import Prelude hiding (read)
import Data.Bytes.Types (MutableBytes(..))
import Data.Word (Word64,Word32)
import System.ByteOrder (Fixed(..),ByteOrder(LittleEndian))
import Socket.Stream.IPv4 (Connection)
import Socket.Stream.IPv4 (Interruptibility(Uninterruptible))
import Socket.Stream.IPv4 (ReceiveException,SendException)
import Data.Primitive (ByteArray,MutablePrimArray(..),MutableByteArray(..))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Control.Exception (Exception)
import GHC.Exts (RealWorld)
import qualified Socket.Stream.Uninterruptible.MutableBytes as SMB
import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM

newtype Queue = Queue { getQueue :: Word32 }
newtype Message = Message { getMessage :: Word64 }

data Batch = Batch
  { alive :: !Bool
  , start :: !Message
  , messages :: !(UnliftedArray ByteArray)
  }

data Status = Status
  { alive :: !Bool
  , next :: !Message
  }

data RoteraException
  = RoteraExceptionSend !(SendException 'Uninterruptible)
  | RoteraExceptionReceive !(ReceiveException 'Uninterruptible)
  | RoteraExceptionProtocol
  deriving stock (Eq,Show)
  deriving anyclass (Exception)

commit ::
     Connection -- ^ Connection to rotera server
  -> Queue -- ^ Queue to push to
  -> IO (Either RoteraException Status)
commit = pingCommitCommon commitIdent

push ::
     Connection -- ^ Connection to rotera server
  -> Queue -- ^ Queue to push to
  -> Int -- ^ Number of messages
  -> Int -- ^ Total size of concatenated messages
  -> MutablePrimArray RealWorld (Fixed 'LittleEndian Word32)
  -> MutableByteArray RealWorld
  -> IO (Either RoteraException ())
push conn (Queue queue) msgCount totalSz msgLens msgs = do
  buf <- PM.newByteArray reqSz
  PM.writeByteArray buf 0
    (Fixed @'LittleEndian pushIdent)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian queue)
  PM.writeByteArray buf 3
    (Fixed @'LittleEndian (fromIntegral msgCount :: Word32))
  -- TODO: consolidate these with a single sendmsg
  SMB.send conn (MutableBytes buf 0 reqSz) >>= \case
    Left err -> pure (Left (RoteraExceptionSend err))
    Right (_ :: ()) -> SMB.send conn (MutableBytes (untype msgLens) 0 (4 * msgCount)) >>= \case
      Left err -> pure (Left (RoteraExceptionSend err))
      Right (_ :: ()) -> SMB.send conn (MutableBytes msgs 0 totalSz) >>= \case
        Left err -> pure (Left (RoteraExceptionSend err))
        Right (_ :: ()) -> pure (Right ())

stream ::
     Connection -- ^ Connection to rotera server
  -> Queue -- ^ Queue to read from
  -> Message -- ^ Earliest message key
  -> Word64 -- ^ Number of messages
  -> Word32 -- ^ Maximum chunk size
  -> IO (Either RoteraException ())
stream conn (Queue queue) (Message firstIdentW) reqCountW msgsPerChunkW = do
  buf <- PM.newByteArray reqSz
  PM.writeByteArray buf 0
    (Fixed @'LittleEndian streamIdent)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian queue)
  PM.writeByteArray buf 3
    (Fixed @'LittleEndian msgsPerChunkW)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian firstIdentW)
  PM.writeByteArray buf 3
    (Fixed @'LittleEndian reqCountW)
  SMB.send conn (MutableBytes buf 0 reqSz) >>= \case
    Left err -> pure (Left (RoteraExceptionSend err))
    Right (_ :: ()) -> pure (Right ())

-- | Pull messages from a socket that was previously told
-- to stream messages with @stream@. The user must stop
-- calling @pull@ once the requested number of messages
-- have been served. Failure to do this will result in
-- blocking forever on @recv@.
pull ::
     Connection
  -> IO (Either RoteraException Batch)
pull !conn = do
  buf <- PM.newByteArray 24
  receiveBatch conn buf

read ::
     Connection -- ^ Connection to rotera server
  -> Queue -- ^ Queue to read from
  -> Message -- ^ Earliest message key
  -> Word64 -- ^ Number of messages
  -> IO (Either RoteraException Batch)
read conn (Queue queue) (Message firstIdentW) reqCountW = do
  buf <- PM.newByteArray reqSz
  PM.writeByteArray buf 0
    (Fixed @'LittleEndian readIdent)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian queue)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian firstIdentW)
  PM.writeByteArray buf 3
    (Fixed @'LittleEndian reqCountW)
  SMB.send conn (MutableBytes buf 0 reqSz) >>= \case
    Left err -> pure (Left (RoteraExceptionSend err))
    Right (_ :: ()) -> receiveBatch conn buf

receiveBatch ::
     Connection
  -> MutableByteArray RealWorld -- header buffer
  -> IO (Either RoteraException Batch)
receiveBatch conn buf = SMB.receiveExactly conn (MutableBytes buf 0 24) >>= \case
  Left err -> pure (Left (RoteraExceptionReceive err))
  Right (_ :: ()) -> do
    Fixed intrIdent <- PM.readByteArray buf 0 :: IO (Fixed 'LittleEndian Word64)
    Fixed msgIdW <- PM.readByteArray buf 1 :: IO (Fixed 'LittleEndian Word64)
    Fixed msgCountW <- PM.readByteArray buf 2 :: IO (Fixed 'LittleEndian Word64)
    let mstillAlive = case intrIdent of
          0x6063977ea508edcc -> Just True
          0x024d91a955128d3a -> Just False
          _ -> Nothing
    case mstillAlive of
      Nothing -> pure (Left RoteraExceptionProtocol)
      Just alive -> do
        let msgCount = fromIntegral msgCountW :: Int
            msgCountByte = msgCount * 4
        szBuf <- PM.newByteArray msgCountByte
        SMB.receiveExactly conn (MutableBytes szBuf 0 msgCountByte) >>= \case
          Left err -> pure (Left (RoteraExceptionReceive err))
          Right _ -> do
            msgs <- PM.unsafeNewUnliftedArray msgCount
            let typedSzBuf = typed szBuf :: MutablePrimArray RealWorld (Fixed 'LittleEndian Word32)
                go !ix = if ix < msgCount
                  then do
                    Fixed szW <- PM.readPrimArray typedSzBuf ix
                    let sz = fromIntegral szW :: Int
                    msg <- PM.newByteArray sz
                    SMB.receiveExactly conn (MutableBytes msg 0 sz) >>= \case
                      Left err -> pure (Left (RoteraExceptionReceive err))
                      Right (_ :: ()) -> do
                        PM.writeUnliftedArray msgs ix
                          =<< PM.unsafeFreezeByteArray msg
                        go (ix + 1)
                  else do
                    messages <- PM.unsafeFreezeUnliftedArray msgs
                    let start = Message msgIdW
                    pure $! Right $! Batch{alive,start,messages}
            go 0


-- | Returns true if the server is still alive and well.
ping ::
     Connection
  -> Queue -- ^ Queue to check the next message of
  -> IO (Either RoteraException Status)
ping = pingCommitCommon pingIdent

-- Ping and commit have nearly the same interface.
pingCommitCommon ::
     Word64 -- ^ Ping/Commit identifier
  -> Connection -- ^ Connection to rotera server
  -> Queue -- ^ Queue to push to
  -> IO (Either RoteraException Status)
pingCommitCommon ident conn (Queue queue) = do
  buf <- PM.newByteArray reqSz
  PM.writeByteArray buf 0
    (Fixed @'LittleEndian ident)
  PM.writeByteArray buf 2
    (Fixed @'LittleEndian queue)
  SMB.send conn (MutableBytes buf 0 reqSz) >>= \case
    Left err -> pure (Left (RoteraExceptionSend err))
    Right (_ :: ()) -> SMB.receiveExactly conn (MutableBytes buf 0 16) >>= \case
      Left err -> pure (Left (RoteraExceptionReceive err))
      Right (_ :: ()) -> do
        Fixed intrIdent <- PM.readByteArray buf 0 :: IO (Fixed 'LittleEndian Word64)
        Fixed next' <- PM.readByteArray buf 1 :: IO (Fixed 'LittleEndian Word64)
        let next = Message next'
        case intrIdent of
          0x6063977ea508edcc -> pure (Right (Status{next,alive=True}))
          0x024d91a955128d3a -> pure (Right (Status{next,alive=False}))
          _ -> pure (Left RoteraExceptionProtocol)

pingIdent :: Word64
pingIdent = 0x44adba9e22c5cf56

readIdent :: Word64
readIdent = 0x7a23663364b9d865

pushIdent :: Word64
pushIdent = 0x70bbf8926f2a1ec6

-- TODO: really calculate this
reqSz :: Int
reqSz = 32

typed :: MutableByteArray s -> MutablePrimArray s a
typed (MutableByteArray x) = MutablePrimArray x

untype :: MutablePrimArray s a -> MutableByteArray s
untype (MutablePrimArray x) = MutableByteArray x

commitIdent :: Word64
commitIdent = 0xfc54160306bf77d4

streamIdent :: Word64
streamIdent = 0xbd4a8ffdc74673dd

