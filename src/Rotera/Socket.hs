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
import Data.ByteString (ByteString)
import Data.Bytes.Types (MutableBytes(..))
import Data.Word (Word16)
import Rotera.Unsafe (Rotera)
import Data.Primitive (MutableByteArray(..),ByteArray)
import Data.Primitive (PrimArray,SmallArray,Prim)
import Data.Word (Word32,Word64)
import GHC.Exts (RealWorld)
import Control.Concurrent (ThreadId)
import Rotera.Unsafe (EventRange(..),Rotera(..))
import System.ByteOrder (Fixed(Fixed),ByteOrder(LittleEndian))
import Socket.Stream.IPv4 (Connection,SendException(..))
import Socket.Stream.IPv4 (CloseException(..),ReceiveException(..))
import Socket.Stream.IPv4 (Interruptibility(..),Peer(..))
import Socket.Stream.IPv4 (AcceptException(..))

import qualified Data.Vector.Primitive.Mutable as PV
import qualified Control.Concurrent.STM as STM
import qualified Net.IPv4 as IPv4
import qualified Socket.Stream.IPv4 as SCK
import qualified Data.Primitive as PM
import qualified Data.ByteString.Char8 as BC
import qualified Data.Bytes.Unsliced as BU
import qualified Socket.Stream.Uninterruptible.MutableBytes as SMB
import qualified Rotera.Nonblocking as NB
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
  !(SmallArray Rotera) -- data for each queue, same length as resolver

data Dynamic = Dynamic
  -- request buffer for sizes of messages
  !(MutableByteArray RealWorld)
  -- request buffer for concatenated payloads, needed
  -- for concurrent ingest
  !(MutableByteArray RealWorld)

-- Sanity checks:
-- * Reject pushes with more than 2^20 messages
-- * Cap reads at 2^18 messages
-- * Cap stream batch sizes at 2^18 messages

-- | Start a rotera server on 127.0.0.1
server ::
     Word16
     -- ^ Port at which the server will listen.
  -> TVar Bool
     -- ^ Interrupt. When this becomes @True@, stop accepting
     -- new connections and gracefully close all open
     -- connections.
  -> PrimArray Word32
     -- ^ Queue resolver
  -> SmallArray Rotera
     -- ^ Metadata for each queue, must be the same
     -- length as the queue resolver.
  -> IO ()
server p intr resolver roteras = do
  e <- SCK.withListener
    Peer{address=IPv4.loopback,port=p}
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
                reqBufStatic <- PM.newByteArray reqSz
                respBuf <- PM.newByteArray 16
                let static = Static
                      intr conn descr reqBufStatic respBuf resolver roteras
                -- No real reason for the reqBuf to start at 64 bytes.
                -- It must be at least 32 bytes to handle requests.
                reqBufDynamic <- PM.newByteArray 64
                -- The payload buffer really should start at zero. Not
                -- for correctness but because there is no way to
                -- estimate the size of what we will be receiving.
                payloadBuf <- PM.newByteArray 0
                handleConnection static (Dynamic reqBufDynamic payloadBuf)
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
handleConnection static@(Static intr conn descr reqBuf respBuf resolver roteras) (Dynamic msgSizes0 payloadBuf0) = do
  SMB.receiveExactly conn (MutableBytes reqBuf 0 reqSz) >>= \case
    Left err -> documentExceptionCommand descr err
    Right _ -> decodeReq reqBuf >>= \case
      Nothing -> BC.putStr $ BC.concat
        [ BU.toByteString descr
        , "[warn] Client sent an invalid request.\n"
        ]
      Just req -> case req of
        Push msgCountW queue -> if msgCountW > 1048576
          then BC.putStr $ BC.concat
              [ BU.toByteString descr
              , "[warn] Client tried to push more than 1048576 messages.\n"
              ]
          else case resolve resolver queue of
            Nothing -> BC.putStr $ BC.concat
              [ BU.toByteString descr
              , "[warn] Client tried to push to non-existing queue.\n"
              ]
            Just queueIx -> do
              let msgCount = fromIntegral msgCountW :: Int
              let r = PM.indexSmallArray roteras queueIx
              msgSizes1 <- ensureCapacity msgSizes0 (msgCount * 4)
              SMB.receiveExactly conn (MutableBytes msgSizes1 0 (msgCount * 4)) >>= \case
                Left err -> documentExceptionPush descr err
                Right (_ :: ()) -> do
                  -- TODO: Attempt to load the payload directly into
                  -- the mmapped region.
                  let msgSzVec :: PV.MVector RealWorld (Fixed 'LittleEndian Word32)
                      msgSzVec = PV.MVector 0 msgCount msgSizes1
                  payloadSz <- vecFoldl
                    (\x y -> pure (x + fromIntegral y))
                    (0 :: Int)
                    msgSzVec
                  payloadBuf1 <- ensureCapacity payloadBuf0 payloadSz
                  SMB.receiveExactly conn (MutableBytes payloadBuf1 0 payloadSz) >>= \case
                    Left err -> documentExceptionPush descr err
                    Right (_ :: ()) -> do
                      Rotera.pushMany r msgSzVec
                        (MutableBytes payloadBuf1 0 payloadSz)
                      handleConnection static (Dynamic msgSizes1 payloadBuf1)
        Read queue msgCountUncroppedW firstIdentW -> case resolve resolver queue of
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to read from non-existing queue.\n"
            ]
          Just queueIx -> do
            msgCountW <- if msgCountUncroppedW > 262144
              then do
                BC.putStr $ BC.concat
                  [ BU.toByteString descr
                  , "[warn] Cropping large read.\n"
                  ]
                pure 262144
              else pure msgCountUncroppedW
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
            NB.readIntoSocketNonblocking r descr firstIdent msgCount conn msgSizes1 >>= \case
              Nothing -> pure ()
              Just actualCount -> do
                BC.putStr $ BC.concat
                  [ BU.toByteString descr
                  , "[debug] Finish sending "
                  , BC.pack (show actualCount)
                  , " messages.\n"
                  ]
                handleConnection static (Dynamic msgSizes1 payloadBuf0)
        ReadStream queue msgsPerChunkUncroppedW firstIdentW msgCountW -> case resolve resolver queue of
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to read from non-existing queue.\n"
            ]
          Just queueIx -> do
            msgsPerChunkW <- if msgsPerChunkUncroppedW > 262144
              then do
                BC.putStr $ BC.concat
                  [ BU.toByteString descr
                  , "[warn] Cropping large read.\n"
                  ]
                pure 262144
              else pure msgsPerChunkUncroppedW
            BC.putStr $ BC.concat
              [ BU.toByteString descr
              , "[debug] Streaming up to "
              , BC.pack (show msgCountW)
              , " messages in chunks of "
              , BC.pack (show msgsPerChunkW)
              , " starting from "
              , BC.pack (show firstIdentW)
              , ".\n"
              ]
            let r = PM.indexSmallArray roteras queueIx
                firstIdent = fromIntegral firstIdentW :: Int
                msgCount = fromIntegral msgCountW :: Int
                msgsPerChunk = fromIntegral msgsPerChunkW :: Int
            msgSizes1 <- ensureCapacity msgSizes0 (24 + (msgsPerChunk * 4))
            -- TODO: Graceful shutdown does not really work correctly
            -- with the streaming interface right now. The problem is
            -- that if we are blocking because we are at the head of
            -- the queue and no new messages are coming in, we never
            -- tell the client that we are done sending stuff. This
            -- is not terribly difficult to fix.
            let go !msgIdent !remaining = if remaining > 0
                  then do
                    intrVal <- STM.readTVarIO intr
                    PM.writeByteArray msgSizes1 0
                      (Fixed @'LittleEndian (intrToIdent intrVal))
                    NB.readIntoSocketBlocking r descr msgIdent (min msgsPerChunk remaining) conn msgSizes1 >>= \case
                      Nothing -> pure ()
                      Just actualCount -> go (msgIdent + actualCount) (remaining - actualCount)
                  else do
                    BC.putStr $ BC.concat
                      [ BU.toByteString descr
                      , "[debug] Finished streaming "
                      , BC.pack (show msgCount)
                      , " messages.\n"
                      ]
                    handleConnection static (Dynamic msgSizes1 payloadBuf0)
            go firstIdent msgCount
        Commit queue -> case resolve resolver queue of
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to commit to non-existing queue.\n"
            ]
          Just queueIx -> do
            let r@Rotera{eventRangeVar} = PM.indexSmallArray roteras queueIx
            Rotera.commit r
            EventRange _ nextEvent <- STM.readTVarIO eventRangeVar
            intrVal <- STM.readTVarIO intr
            PM.writeByteArray respBuf 0
              (Fixed @'LittleEndian (intrToIdent intrVal))
            PM.writeByteArray respBuf 1
              (Fixed @'LittleEndian (fromIntegral nextEvent :: Word64))
            SMB.send conn (MutableBytes respBuf 0 16) >>= \case
              Left err -> documentPingSendException descr err
              Right _ -> handleConnection static (Dynamic msgSizes0 payloadBuf0)
        Ping queue -> case resolve resolver queue of
          -- This is very similar to the code for handling Commit.
          -- Consider refactoring.
          Nothing -> BC.putStr $ BC.concat
            [ BU.toByteString descr
            , "[warn] Client tried to ping non-existing queue.\n"
            ]
          Just queueIx -> do
            let Rotera{eventRangeVar} = PM.indexSmallArray roteras queueIx
            EventRange _ nextEvent <- STM.readTVarIO eventRangeVar
            intrVal <- STM.readTVarIO intr
            PM.writeByteArray respBuf 0
              (Fixed @'LittleEndian (intrToIdent intrVal))
            PM.writeByteArray respBuf 1
              (Fixed @'LittleEndian (fromIntegral nextEvent :: Word64))
            SMB.send conn (MutableBytes respBuf 0 16) >>= \case
              Left err -> documentPingSendException descr err
              Right _ -> handleConnection static (Dynamic msgSizes0 payloadBuf0)

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

-- TODO: Really calculate this. I think this number is actually correct.
reqSz :: Int
reqSz = 32

data Req
  = Push
      !Word32 -- queue id
      !Word32 -- number of messages
  | Read
      !Word32 -- queue id
      !Word32 -- maximum number of messages
      !Word64 -- first ident
  | ReadStream
      !Word32 -- queue id
      !Word32 -- maximum messages per chunk
      !Word64 -- first ident
      !Word64 -- exact number of messages
  | Commit
      !Word32 -- queue id
  | Ping
      !Word32 -- queue id

decodeReq :: MutableByteArray RealWorld -> IO (Maybe Req)
decodeReq arr = do
  Fixed ident <- PM.readByteArray arr 0 :: IO (Fixed 'LittleEndian Word64)
  case ident of
    0x70bbf8926f2a1ec6 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word32)
      pure (Just (Push msgCount queue))
    0x44adba9e22c5cf56 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      pure (Just (Ping queue))
    0xfc54160306bf77d4 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      pure (Just (Commit queue))
    0x7a23663364b9d865 -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word32)
      Fixed firstIdent <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word64)
      pure (Just (Read queue msgCount firstIdent))
    0xbd4a8ffdc74673dd -> do
      Fixed queue <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word32)
      Fixed msgsPerChunk <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word32)
      Fixed firstIdent <- PM.readByteArray arr 2 :: IO (Fixed 'LittleEndian Word64)
      Fixed msgCount <- PM.readByteArray arr 3 :: IO (Fixed 'LittleEndian Word64)
      pure (Just (ReadStream queue msgsPerChunk firstIdent msgCount))
    _ -> pure Nothing

intrToIdent :: Bool -> Word64
intrToIdent = \case
  True -> shutdownIdent
  False -> aliveIdent

_readIdent :: Word64
_readIdent = 0x7a23663364b9d865

_streamIdent :: Word64
_streamIdent = 0xbd4a8ffdc74673dd

_pingIdent :: Word64
_pingIdent = 0x44adba9e22c5cf56

_pushIdent :: Word64
_pushIdent = 0x70bbf8926f2a1ec6

shutdownIdent :: Word64
shutdownIdent = 0x024d91a955128d3a

aliveIdent :: Word64
aliveIdent = 0x6063977ea508edcc

_commitIdent :: Word64
_commitIdent = 0xfc54160306bf77d4

vecFoldl :: Prim a => (b -> a -> IO b) -> b -> PV.MVector RealWorld a -> IO b
vecFoldl f !b0 !v = go 0 b0 where
  go !ix !b = if ix < PV.length v
    then (f b =<< PV.unsafeRead v ix) >>= go (ix + 1)
    else pure b
