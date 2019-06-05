{-# language BangPatterns #-}
{-# language NamedFieldPuns #-}
{-# language LambdaCase #-}
{-# language OverloadedStrings #-}
{-# language DuplicateRecordFields #-}

import Prelude hiding (Read,id)

import Control.Exception (throwIO)
import Control.Monad (when,forM_)
import Data.Primitive (PrimArray,MutablePrimArray,Prim)
import Data.Word (Word32,Word64)
import GHC.Exts (RealWorld)
import Options.Applicative ((<**>))
import Rotera.Client (Batch(..),Queue(..),Status(..),Message(..))
import Socket.Stream.IPv4 (Interruptibility(..),Peer(..))
import System.IO (stdin)
import System.ByteOrder (Fixed(..))

import qualified Data.List.Split as Split
import qualified Data.ByteString as B
import qualified Data.Bytes.Mutable.Unsliced as MB
import qualified Data.Bytes.Unsliced as BU
import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified GHC.Exts as E
import qualified GHC.OldList as L
import qualified Net.IPv4 as IPv4
import qualified Options.Applicative as P
import qualified Rotera.Client as R
import qualified Socket.Stream.IPv4 as SCK

main :: IO ()
main = do
  cmd <- P.execParser $ P.info
    (commandParser <**> P.helper)
    P.fullDesc
  run cmd

run :: Command -> IO ()
run cmd = do
  e <- SCK.withConnection
    Peer{address=IPv4.loopback,port=8245}
    (\e () -> case e of
      Left err -> throwIO err
      Right () -> pure ()
    )
    (\conn -> case cmd of
      CommandPing Ping{queue} -> R.ping conn queue >>= \case
        Left err -> throwIO err
        Right Status{alive,next=R.Message msgId} -> if alive
          then putStrLn $ "Next message is " ++ show msgId ++ ". Server is alive."
          else putStrLn $ "Next message is " ++ show msgId ++ ". Server shutting down."
      CommandPush Push{queue,chunk,commit} -> do
        strs <- fmap (L.filter (not . T.null) . L.map T.strip . T.splitOn (T.singleton '\n')) (TIO.hGetContents stdin)
        let bstrs' = L.map TE.encodeUtf8 strs
        forM_ (Split.chunksOf chunk bstrs') $ \bstrs -> do
          let lens = L.map ((fromIntegral :: Int -> Word32) . B.length) bstrs
              bstr = B.concat bstrs
          mbstr <- MB.thawByteString bstr
          let msgLens' = PM.mapPrimArray Fixed (E.fromList lens)
              msgCount = PM.sizeofPrimArray msgLens'
          msgLens <- thawPrimArray msgLens' 0 msgCount
          R.push conn queue msgCount (B.length bstr) msgLens mbstr
            >>= either throwIO pure
        when commit $ do
          _ <- R.commit conn queue
          pure ()
      CommandRead Read{queue,id,count,printIds} -> do
        Batch{start,messages} <- either throwIO pure
          =<< R.read conn queue id count
        PM.itraverseUnliftedArray_
          ( \ix msg -> do
            let prefix = if printIds
                  then T.pack (lpad 11 (show (R.getMessage start + fromIntegral ix)) ++ " ")
                  else T.empty
            TIO.putStr prefix
            case TE.decodeUtf8' (BU.toByteString msg) of
              Left _ -> TIO.putStr "<non-utf8>\n"
              Right t -> TIO.putStrLn t
          ) messages
      CommandStream Stream{queue,id,count,printIds,chunk} -> do
        either throwIO pure
          =<< R.stream conn queue id count chunk
        let go :: Word64 -> IO ()
            go !receivedMsgs = if receivedMsgs < count
              then do
                Batch{start,messages,alive} <- either throwIO pure =<< R.pull conn
                PM.itraverseUnliftedArray_
                  ( \ix msg -> do
                    let prefix = if printIds
                          then T.pack (lpad 11 (show (R.getMessage start + fromIntegral ix)) ++ " ")
                          else T.empty
                    TIO.putStr prefix
                    case TE.decodeUtf8' (BU.toByteString msg) of
                      Left _ -> TIO.putStr "<non-utf8>\n"
                      Right t -> TIO.putStrLn t
                  ) messages
                if alive
                  then go (receivedMsgs + fromIntegral (PM.sizeofUnliftedArray messages))
                  else pure ()
              else pure ()
        go 0
    )
  case e of
    Left err -> throwIO err
    Right () -> pure ()

data Command
  = CommandPing !Ping
  | CommandRead !Read
  | CommandStream !Stream
  | CommandPush !Push

newtype Ping = Ping
  { queue :: Queue
  }

data Push = Push
  { commit :: !Bool
  , chunk :: !Int
  , queue :: !Queue
  }

data Read = Read
  { id :: !Message
  , count :: !Word64
  , printIds :: !Bool
  , queue :: !Queue
  }

data Stream = Stream
  { id :: !Message
  , count :: !Word64
  , chunk :: !Word32
  , printIds :: !Bool
  , queue :: !Queue
  }

commandParser :: P.Parser Command
commandParser = P.hsubparser $ mconcat
  [ P.command "ping" $ P.info
      (CommandPing <$> pingParser)
      (P.progDesc "Ping the server")
  , P.command "read" $ P.info
      (CommandRead <$> readParser)
      (P.progDesc "Read messages from the queue")
  , P.command "stream" $ P.info
      (CommandStream <$> streamParser)
      (P.progDesc "Stream messages from the queue")
  , P.command "push" $ P.info
      (CommandPush <$> pushParser)
      (P.progDesc "Push messages into the queue")
  ]

pingParser :: P.Parser Ping
pingParser = Ping
  <$> fmap Queue
      ( P.argument P.auto
      ( P.metavar "QUEUE"
     <> P.help "Queue identifier"
      )
      )

pushParser :: P.Parser Push
pushParser = Push
  <$> fmap not
      ( P.switch
      ( P.long "no-commit"
     <> P.help "Do not commit the messages to disk"
      )
      )
  <*> P.option P.auto
      ( P.long "chunk"
     <> P.short 'c'
     <> P.metavar "INT"
     <> P.value 24
     <> P.help "Messages per push"
      )
  <*> fmap Queue
      ( P.argument P.auto
      ( P.metavar "QUEUE"
     <> P.help "Queue identifier"
      )
      )

readParser :: P.Parser Read
readParser = Read
  <$> fmap Message
      ( P.option P.auto
      ( P.long "id"
     <> P.short 'i'
     <> P.metavar "ID"
     <> P.value 0
     <> P.help "Starting event identifier"
      )
      )
  <*> P.option P.auto
      ( P.long "count"
     <> P.short 'n'
     <> P.metavar "INT"
     <> P.value 1
     <> P.help "Number of events to read"
      )
  <*> P.switch
      ( P.long "print-ids"
     <> P.short 'p'
     <> P.help "Nontextual data"
      )
  <*> fmap Queue
      ( P.argument P.auto
      ( P.metavar "QUEUE"
     <> P.help "Queue identifier"
      )
      )

streamParser :: P.Parser Stream
streamParser = Stream
  <$> fmap Message
      ( P.option P.auto
      ( P.long "id"
     <> P.short 'i'
     <> P.metavar "ID"
     <> P.value 0
     <> P.help "Starting event identifier"
      )
      )
  <*> P.option P.auto
      ( P.long "count"
     <> P.short 'n'
     <> P.metavar "INT"
     <> P.value 1
     <> P.help "Total number of events to read"
      )
  <*> P.option P.auto
      ( P.long "chunk"
     <> P.short 'c'
     <> P.metavar "INT"
     <> P.value 64
     <> P.help "Number of events per batch"
      )
  <*> P.switch
      ( P.long "print-ids"
     <> P.short 'p'
     <> P.help "Nontextual data"
      )
  <*> fmap Queue
      ( P.argument P.auto
      ( P.metavar "QUEUE"
     <> P.help "Queue identifier"
      )
      )

lpad :: Int -> [Char] -> [Char]
lpad m xs = L.replicate (m - length ys) '0' ++ ys
  where ys = take m xs

thawPrimArray :: Prim a
  => PrimArray a -> Int -> Int -> IO (MutablePrimArray RealWorld a)
thawPrimArray src off len = do
  dst <- PM.newPrimArray len
  PM.copyPrimArray dst 0 src off len
  pure dst
