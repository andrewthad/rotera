{-# language GADTs #-}
{-# language NamedFieldPuns #-}
{-# language ScopedTypeVariables #-}
{-# language DataKinds #-}
{-# language LambdaCase #-}
{-# language BangPatterns #-}
{-# language TypeApplications #-}
{-# language OverloadedStrings #-}

module Rotera.Socket
  ( server
  ) where

import Control.Concurrent.STM (TVar)
import Control.Exception (throwIO)
import Data.Bytes.Types (MutableBytes(..))
import Rotera.Unsafe (Rotera)
import Data.ByteString (ByteString)
import Data.IORef (IORef,readIORef)
import Data.Primitive (MutableByteArray(..),ByteArray,MutablePrimArray(..))
import Data.Primitive (PrimArray,SmallArray,Prim)
import Data.Word (Word32,Word64)
import GHC.Exts (RealWorld)
import Control.Concurrent (ThreadId)
import System.ByteOrder (Fixed(Fixed),ByteOrder(LittleEndian))
import Socket.Stream.IPv4 (Connection,SendException(..))
import Socket.Stream.IPv4 (CloseException(..),ReceiveException(..))
import Socket.Stream.IPv4 (Interruptibility(..),Peer(..))
import Socket.Stream.IPv4 (AcceptException(..))

import qualified Control.Concurrent.STM as STM
import qualified Net.IPv4 as IPv4
import qualified Socket.Stream.IPv4 as SCK
import qualified Data.Primitive as PM
import qualified Data.ByteString.Char8 as BC
import qualified Data.Bytes.Unsliced as BU
import qualified Socket.Stream.Uninterruptible.MutableBytes as SMB
import qualified Rotera.Nonblocking as NB
import qualified GHC.Exts as E
import qualified Rotera

-- TODO: Collapse msgSizes and respBuf into a single
-- field.
data Static = Static
  !(TVar Bool)
  -- Interruptedness. If true, tell clients to disconnect
  !Connection -- connection
  !ByteArray -- description of client with square brackets
  !(MutableByteArray RealWorld)
  -- request buffer, holds one request
  !(MutableByteArray RealWorld)
  -- response buffer, exactly 8 bytes
  !(PrimArray Word32) -- queue resolver
  !(SmallArray Rotera) -- data for each queue

data Dynamic = Dynamic
  -- request buffer for sizes of messages
  !(MutableByteArray RealWorld)

server ::
     TVar Bool
     -- ^ Interrupt. When this becomes @True@, stop accepting
     -- new connections and gracefully close all open
     -- connections.
  -> String
     -- ^ Path to rotera
  -> IO ()
server intr path = do
  r <- Rotera.open path
  e <- SCK.withListener
    Peer{address=IPv4.loopback,port=8245}
    (\lstn prt -> do
      BC.putStr $ BC.concat
        [ "[localhost][info] Listening on port "
        , BC.pack (show prt)
        , "\n"
        ]
      active <- STM.newTVarIO 0
      let go = do
            e <- SCK.interruptibleForkAcceptedUnmasked
              active intr lstn
              (\e descr -> case e of
                Left ClosePeerContinuedSending -> do
                  BC.putStr $ BC.concat
                    [ BU.toByteString descr
                    , "[warn] Continued sending.\n"
                    ]
                Right _ -> pure ()
              )
              (\conn peer -> do
                descr <- describePeer peer
                BC.putStr $ BC.concat
                  [ BU.toByteString descr
                  , "[info] Established connection.\n"
                  ]
                reqBuf <- PM.newByteArray reqSz
                respBuf <- PM.newByteArray 8
                let resolver = E.fromList [42]
                let rs = E.fromList [r]
                let static = Static
                      intr conn descr reqBuf respBuf resolver rs
                reqBuf <- PM.newByteArray 64 -- no reason
                handleConnection static (Dynamic reqBuf)
                pure descr
              )
            case e of
              Right (_ :: ThreadId) -> go
              Left err -> case err of
                AcceptConnectionAborted -> do
                  BC.putStr "[localhost][info] Connection aborted by client.\n"
                  go
                AcceptFileDescriptorLimit -> do
                  -- Gracefully bring down the server if there are no
                  -- more file descriptors available.
                  -- This might not be the best choice.
                  STM.atomically (STM.writeTVar intr True)
                  BC.putStr "[localhost][crit] File descriptor limit. Shutting down.\n"
                AcceptFirewalled -> do
                  STM.atomically (STM.writeTVar intr True)
                  BC.putStr "[localhost][crit] Firewall prevents accepting connection. Shutting down.\n"
                AcceptInterrupted -> do
                  BC.putStr "[localhost][info] Beginning graceful shutdown.\n"
      go
      STM.atomically $ do
        n <- STM.readTVar active
        STM.check (n <= 0)
    )
  case e of
    Left err -> throwIO err
    Right () -> pure ()

describePeer :: Peer -> IO ByteArray
describePeer (Peer{address,port}) =
  BU.fromByteString $ BC.concat
    [ "["
    , IPv4.encodeUtf8 address
    , ":"
    , BC.pack (show port)
    , "]"
    ]

-- This continues to service requests forever until the
-- client decides to close the connection or until a graceful
-- shutdown is requested or the client errors out.
handleConnection ::
     Static
  -> Dynamic
  -> IO ()
handleConnection static@(Static intr conn descr reqBuf respBuf resolver roteras) (Dynamic msgSizes0) = do
  SMB.receiveExactly conn (MutableBytes reqBuf 0 reqSz) >>= \case
    Left err -> documentExceptionCommand descr err
    Right _ -> decodeReq reqBuf >>= \case
      Nothing -> BC.putStr $ BC.concat
        [ BU.toByteString descr
        , "[warn] Client sent an invalid request.\n"
        ]
      Just req -> case req of
        Push msgCountW queue -> case resolve resolver queue of
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to push to non-existing queue.\n"
            ]
          Just queueIx -> do
            let msgCount = fromIntegral msgCountW :: Int
            msgSizes1 <- ensureCapacity msgSizes0 (msgCount * 4)
            SMB.receiveExactly conn (MutableBytes msgSizes1 0 (msgCount * 4)) >>= \case
              Left err -> documentExceptionPush descr err
              Right _ -> do
                -- TODO: actually handle this
                handleConnection static (Dynamic msgSizes1)
        Read queue firstIdentW msgCountW -> case resolve resolver queue of
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to read from non-existing queue.\n"
            ]
          Just queueIx -> do
            BC.putStr $ BC.concat
              [ BU.toByteString descr
              , "[debug] Reading up to "
              , BC.pack (show msgCountW)
              , " messages starting from "
              , BC.pack (show firstIdentW)
              , ".\n"
              ]
            let r = PM.indexSmallArray roteras queueIx
                firstIdent = fromIntegral firstIdentW :: Int
                msgCount = fromIntegral msgCountW :: Int
            msgSizes1 <- ensureCapacity msgSizes0 (24 + (msgCount * 4))
            intrVal <- STM.readTVarIO intr
            PM.writeByteArray msgSizes1 0
              (Fixed @'LittleEndian (intrToIdent intrVal))
            NB.readIntoSocket r descr firstIdent msgCount conn msgSizes1 >>= \case
              Nothing -> pure ()
              Just actualCount -> do
                BC.putStr $ BC.concat
                  [ BU.toByteString descr
                  , "[debug] Finish sending "
                  , BC.pack (show actualCount)
                  , " messages.\n"
                  ]
                handleConnection static (Dynamic msgSizes1)
        Ping -> do
          intrVal <- STM.readTVarIO intr
          PM.writeByteArray respBuf 0
            (Fixed @'LittleEndian (intrToIdent intrVal))
          SMB.send conn (MutableBytes respBuf 0 8) >>= \case
            Left err -> documentPingSendException descr err
            Right _ -> handleConnection static (Dynamic msgSizes0)

resolve :: PrimArray Word32 -> Word32 -> Maybe Int
resolve arr w = go (PM.sizeofPrimArray arr - 1) where
  go !ix = if ix >= 0
    then if PM.indexPrimArray arr ix == w
      then Just ix
      else go (ix - 1)
    else Nothing

-- This does not preserve the elements.
ensureCapacity ::
     MutableByteArray RealWorld
  -> Int
  -> IO (MutableByteArray RealWorld)
ensureCapacity m needed = do
  actual <- PM.getSizeofMutableByteArray m
  if needed > actual
    then PM.newByteArray needed
    else pure m

untype :: MutablePrimArray s a -> MutableByteArray s
untype (MutablePrimArray x) = MutableByteArray x

printPrefixed :: ByteArray -> ByteString -> IO ()
printPrefixed descr msg = BC.putStr $ BC.concat
  [ BU.toByteString descr
  , msg
  ]

documentExceptionCommand :: ByteArray -> ReceiveException 'Uninterruptible -> IO ()
documentExceptionCommand descr = \case
  ReceiveShutdown -> printPrefixed descr
    "[info] Client gracefully closed the connection.\n"
  ReceiveReset -> printPrefixed descr
    "[warn] Client reset the connection instead of sending command.\n"
  ReceiveHostUnreachable -> printPrefixed descr
    "[warn] Client became unreachable instead of sending command.\n"

documentExceptionPush :: ByteArray -> ReceiveException 'Uninterruptible -> IO ()
documentExceptionPush descr = \case
  ReceiveShutdown -> printPrefixed descr
    "[warn] Client closed the connection while sending message sizes.\n"
  ReceiveReset -> printPrefixed descr
    "[warn] Client reset the connection while sending message sizes.\n"
  ReceiveHostUnreachable -> printPrefixed descr
    "[warn] Client became unreachable while sending message sizes.\n"

documentPingSendException :: ByteArray -> SendException 'Uninterruptible -> IO ()
documentPingSendException descr = \case
  SendShutdown -> printPrefixed descr
    "[warn] Client closed connection while server was responding to ping.\n"
  SendReset -> printPrefixed descr
    "[warn] Client reset connection while server was responding to ping.\n"

-- TODO: really calculate this
reqSz :: Int
reqSz = 32

data Req
  = Push
      !Word32 -- queue id
      !Word32 -- number of messages
  | Read
      !Word32 -- queue id
      !Word64 -- first ident
      !Word64 -- maximum number of messages
  | ReadStream
      !Word32 -- queue id
      !Word64 -- first ident
      !Word64 -- exact number of messages
  | Ping

decodeReq :: MutableByteArray RealWorld -> IO (Maybe Req)
decodeReq arr = do
  Fixed ident <- PM.readByteArray arr 0 :: IO (Fixed 'LittleEndian Word64)
  case ident of
    0x70bbf8926f2a1ec6 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word32)
      pure (Just (Push msgCount queue))
    0x44adba9e22c5cf56 -> pure (Just Ping)
    0x7a23663364b9d865 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed firstIdent <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word64)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word64)
      pure (Just (Read queue firstIdent msgCount))
    0xbd4a8ffdc74673dd -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed firstIdent <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word64)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word64)
      pure (Just (ReadStream queue firstIdent msgCount))
    _ -> pure Nothing
  
intrToIdent :: Bool -> Word64
intrToIdent = \case
  True -> shutdownIdent
  False -> aliveIdent

readIdent :: Word64
readIdent = 0x7a23663364b9d865

readStreamIdent :: Word64
readStreamIdent = 0xbd4a8ffdc74673dd

pingIdent :: Word64
pingIdent = 0x44adba9e22c5cf56

pushIdent :: Word64
pushIdent = 0x70bbf8926f2a1ec6

shutdownIdent :: Word64
shutdownIdent = 0x024d91a955128d3a

aliveIdent :: Word64
aliveIdent = 0x6063977ea508edcc
