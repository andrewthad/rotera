{-# language BangPatterns #-}
{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}
{-# language TypeApplications #-}

import Control.Monad.IO.Class (liftIO)
import Data.IORef
import Data.Word (Word16, Word32, Word64)
import Prelude hiding (read)
import Rotera.Client (RoteraException,Queue(..),Batch(..),Status(..))
import Socket.Stream.IPv4 (Peer(..))
import System.ByteOrder (Fixed(..))
import System.Console.Repline
import System.Exit (exitSuccess,exitFailure)
import System.IO.Unsafe (unsafePerformIO)
import Text.Read (readMaybe)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Encodings as BE
import qualified Data.Bytes.Mutable.Unsliced as MB
import qualified Data.Bytes.Unsliced as BU
import qualified Data.Char as Char
import qualified Data.Primitive.Contiguous as C
import qualified Data.Primitive.Unlifted.Array as PM
import qualified GHC.OldList as L
import qualified Net.IPv4 as IPv4
import qualified Options.Applicative as P
import qualified Rotera.Client as R
import qualified Socket.Stream.IPv4 as SCK

data OnOff = On | Off
  deriving (Eq, Show)

queueRef :: IORef Queue
queueRef = unsafePerformIO $ newIORef (Queue 0)
{-# noinline queueRef #-}

printIdsRef :: IORef OnOff
printIdsRef = unsafePerformIO $ newIORef On
{-# noinline printIdsRef #-}

batchRef :: IORef Word32
batchRef = unsafePerformIO $ newIORef 0
{-# noinline batchRef #-}

main :: IO ()
main = do
  port <- P.execParser $ P.info
    (portParser P.<**> P.helper)
    P.fullDesc
  run port

run :: Word16 -> IO ()
run p = do
  e <- SCK.withConnection
    Peer{address=IPv4.loopback,port=p}
    (\e () -> case e of
      Left err -> do
        putStr $ "Unable to establish connection to rotera-server.\n"
        putStr $ "Error was: " <> show err <> "\n"
        exitFailure
      Right () -> pure ()
    )
    repl
  case e of
    Left err -> do
      putStr $ "Failure when executing repl.\n"
      putStr $ "Error was: " <> show err <> "\n"
      exitFailure
    Right () -> pure ()

repl :: SCK.Connection -> IO ()
repl conn = evalRepl (pure ">>> ") commandF (options conn) (Just ':') (Word completer) ini

type Repl a = HaskelineT IO a

ini :: Repl ()
ini = liftIO $ putStr "Welcome to rotera-repl.\n\n"

commandF :: String -> Repl ()
commandF input = liftIO $ putStr $ mal input

completer :: Applicative m => WordCompleter m
completer n = do
  let names =
        [ "queue"
        , "print-ids"
        , "batch"
        , "read"
        , "read-batch"
        , "push"
        , "ping"
        , "current"
        , "help"
        , "quit"
        ]
  pure $ L.filter (L.isPrefixOf n) (names ++ map (':' :) names)

helpStr :: String
helpStr = mconcat
  [ "Commands:\n"
  , "    :queue <x: u32>      -- update queue pointer to `x`\n"
  , "    :print-ids {on|off}  -- whether or not to print message ids\n"
  , "    :batch <x: u32>      -- update default number of messages to read to `x`\n"
  , "    :read                -- read `batch` number of messages\n"
  , "    :read-batch <x: u32> -- read `x` number of messages\n"
  , "    :push <msg>          -- push `msg` and commit\n"
  , "    :ping                -- ping rotera-server and return the most recent commit.\n"
  , "    :current             -- show current settings.\n"
  , "    :help, :h            -- display this help menu\n"
  , "    :quit, :q            -- close the connection to rotera-server and quit the repl\n"
  ]

options :: SCK.Connection -> [(String, [String] -> Repl ())]
options conn =
  [ ("help", help), ("h", help)
  , ("quit", quit), ("q", quit)
  , ("queue", queue)
  , ("read", read conn)
  , ("read-batch", readBatch conn)
  , ("push", push conn)
  , ("ping", ping conn)
  , ("current", current)
  , ("batch", batch)
  , ("print-ids", printIds)
  ]

help :: [String] -> Repl ()
help = const $ liftIO $ putStr helpStr

quit :: [String] -> Repl ()
quit = const $ liftIO $ do
  putStr "Exiting rotera-repl...\n"
  exitSuccess

queue :: [String] -> Repl ()
queue xs = liftIO $ go xs
  where
    go (x:[]) = do
      case readMaybe @Word32 x of
        Nothing -> do
          putStr (mal "queue")
        Just newQueue -> do
          Queue currentQueue <- readIORef queueRef
          writeIORef queueRef (Queue newQueue)
          putStr $
            "Overwriting old value of `queue` ("
            <> show currentQueue
            <> ") with new value of "
            <> show newQueue
            <> "\n"
    go _ = putStr (mal "queue")

printIds :: [String] -> Repl ()
printIds xs = liftIO $ go xs where
  go (x:[]) = case (map Char.toLower x) of
    "on" -> do
      writeIORef printIdsRef On
    "off" -> do
      writeIORef printIdsRef Off
    x' -> do
      putStr $
        x' <> " isn't one of {on,off}."
  go _ = putStr (mal "print-ids")

batch :: [String] -> Repl ()
batch xs = liftIO $ go xs where
  go (x:[]) = do
    case readMaybe @Word32 x of
      Nothing -> do
        putStr (mal "batch")
      Just newBatch -> do
        currentBatch <- readIORef batchRef
        writeIORef batchRef newBatch
        putStr $
          "Overwriting old value of `batch` ("
          <> show currentBatch
          <> ") with new value of "
          <> show newBatch
          <> "\n"
  go _ = putStr (mal "batch")

current :: [String] -> Repl ()
current [] = liftIO $ do
  Queue currentQueue <- readIORef queueRef
  currentPrintIds <- readIORef printIdsRef
  currentBatch <- readIORef batchRef
  putStr $ mconcat
    [ "Queue:       " <> show currentQueue <> "\n"
    , "PrintIds:    " <> show currentPrintIds <> "\n"
    , "Batch:       " <> show currentBatch <> "\n"
    ]
current _ = liftIO $ putStr (mal "current")

ping :: SCK.Connection -> [String] -> Repl ()
ping conn [] = liftIO $ do
  readIORef queueRef >>= R.ping conn >>= \case
    Left err -> do
      putStr $ errPing err
    Right R.Status{alive,next=R.Message msgId} -> if alive
      then putStr $ "Next message is " ++ show msgId ++ ". Server is alive.\n"
      else putStr $ "Next message is " ++ show msgId ++ ". Server shutting down.\n"
ping _ _ = liftIO $ putStr (mal "ping")

push :: SCK.Connection -> [String] -> Repl ()
push conn (x:_) = liftIO $ do
  let bstr = BC8.pack x
      len = fromIntegral @Int @Word32 . B.length $ bstr
  mbstr <- MB.thawByteString bstr
  let msgLens' = C.singleton (Fixed len)
      msgCount = 1
  msgLens <- C.thaw msgLens' 0 msgCount
  currentQueue <- readIORef queueRef
  e <- R.push conn currentQueue msgCount (B.length bstr) msgLens mbstr
  case e of
    Left err -> do
      putStr $ "Unable to push message to rotera-server.\n"
      putStr $ "Error was: " <> show err
    Right () -> do
      () <$ R.commit conn currentQueue
push _ _ = liftIO $ putStr (mal "push")

readBatch :: SCK.Connection -> [String] -> Repl ()
readBatch conn (x:_) = liftIO $ do
  case readMaybe @Word32 x of
    Nothing -> putStr (mal "read-batch")
    Just b -> do
      currentQueue <- readIORef queueRef
      currentPrintIds <- readIORef printIdsRef
      R.ping conn currentQueue >>= \case
        Left err -> do
          putStr $ errPing err
        Right R.Status{next} -> do
          R.read conn currentQueue next b >>= \case
            Left err -> do
              putStr $ errRead err
            Right Batch{start,messages} -> do
              flip PM.itraverseUnliftedArray_ messages $ \ix msg -> do
                let prefix = if currentPrintIds == On
                      then lpad 11 (show (R.getMessage start + fromIntegral ix)) ++ " "
                      else ""
                putStr prefix
                let msg' = BU.toByteString msg
                if BE.isUtf8 msg'
                  then B.putStrLn msg
                  else putStr errNonUtf8
readBatch _ _ = liftIO $ putStr (mal "read-batch")

read :: SCK.Connection -> [String] -> Repl ()
read conn [] = liftIO $ do
  b <- readIORef batchRef
  currentQueue <- readIORef queueRef
  currentPrintIds <- readIORef printIdsRef
  R.ping conn currentQueue >>= \case
    Left err -> do
      putStr $ errPing err
    Right R.Status{next} -> do
      R.read conn currentQueue next b >>= \case
        Left err -> do
          putStr $ errRead err
        Right Batch{start,messages} -> do
          flip PM.itraverseUnliftedArray_ messages $ \ix msg -> do
            let prefix = if currentPrintIds == On
                  then lpad 11 (show (R.getMessage start + fromIntegral ix)) ++ " "
                  else ""
            putStr prefix
            let msg' = BU.toByteString msg
            if BE.isUtf8 msg'
              then B.putStrLn msg
              else putStr errNonUtf8
read _ _ = liftIO $ putStr (mal "read")

errNonUtf8 :: String
errNonUtf8 = "Encountered non-UTF-8 text when reading from rotera-server.\n"

errPing :: RoteraException -> String
errPing err = "The server is unreachable.\n"
  <> "Socket error: " <> show err <> "\n"

errRead :: RoteraException -> String
errRead err = "Cannot read batch.\n"
  <> "Error was: " <> show err <> "\n"

portParser :: P.Parser Word16
portParser = P.option P.auto
  ( P.long "port"
 <> P.short 'p'
 <> P.metavar "PORT"
 <> P.value 8245
 <> P.showDefault
 <> P.help "Port where rotera-server is running"
  )

lpad :: Int -> [Char] -> [Char]
lpad m xs = L.replicate (m - length ys) '0' ++ ys
  where ys = take m xs

mal :: String -> String
mal cmd = "malformed `" <> cmd <> "` command. type `:help` for more information.\n"
