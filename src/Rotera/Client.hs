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
  , Queue(..)
  , Message(..)
  , Batch(..)
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

data RoteraException
  = RoteraExceptionSend !(SendException 'Uninterruptible)
  | RoteraExceptionReceive !(ReceiveException 'Uninterruptible)
  | RoteraExceptionProtocol
  deriving stock (Eq,Show)
  deriving anyclass (Exception)

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
    Right (_ :: ()) -> SMB.receiveExactly conn (MutableBytes buf 0 24) >>= \case
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
  -> IO (Either RoteraException Bool)
ping conn = do
  buf <- PM.newByteArray reqSz
  PM.writeByteArray buf 0
    (Fixed @'LittleEndian pingIdent)
  SMB.send conn (MutableBytes buf 0 reqSz) >>= \case
    Left err -> pure (Left (RoteraExceptionSend err))
    Right _ -> SMB.receiveExactly conn (MutableBytes buf 0 8) >>= \case
      Left err -> pure (Left (RoteraExceptionReceive err))
      Right _ -> do
        Fixed ident <- PM.readByteArray buf 0 ::
          IO (Fixed 'LittleEndian Word64)
        case ident of
          0x6063977ea508edcc -> pure (Right True)
          0x024d91a955128d3a -> pure (Right False)
          _ -> pure (Left RoteraExceptionProtocol)

pingIdent :: Word64
pingIdent = 0x44adba9e22c5cf56

readIdent :: Word64
readIdent = 0x7a23663364b9d865

-- TODO: really calculate this
reqSz :: Int
reqSz = 32

typed :: MutableByteArray s -> MutablePrimArray s a
typed (MutableByteArray x) = MutablePrimArray x
