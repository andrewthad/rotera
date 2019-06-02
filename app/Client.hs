{-# language BangPatterns #-}
{-# language NamedFieldPuns #-}
{-# language LambdaCase #-}
{-# language OverloadedStrings #-}

import Prelude hiding (Read,id)

import qualified Rotera.Client as R
import qualified Options.Applicative as P
import qualified Socket.Stream.IPv4 as SCK
import qualified Net.IPv4 as IPv4
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified GHC.OldList as L
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.Text as T
import qualified Data.Bytes.Unsliced as BU
import Rotera.Client (Batch(..))
import Data.Word (Word32,Word64)
import Control.Exception (throwIO)
import Options.Applicative ((<**>))
import Socket.Stream.IPv4 (Interruptibility(..),Peer(..))

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
      CommandPing -> R.ping conn >>= \case
        Left err -> throwIO err
        Right alive -> if alive
          then putStrLn "Server is alive."
          else putStrLn "Server is shutting down."
      CommandRead Read{queue,id,count,printIds} -> do
        Batch{start,messages} <- either throwIO pure
          =<< R.read conn (R.Queue queue) (R.Message id) count
        PM.itraverseUnliftedArray_
          ( \ix msg -> do
            let prefix = if printIds
                  then T.pack (lpad 9 (show (R.getMessage start + fromIntegral ix)) ++ " ")
                  else T.empty
            TIO.putStr prefix
            case TE.decodeUtf8' (BU.toByteString msg) of
              Left _ -> TIO.putStr "<non-utf8>\n"
              Right t -> TIO.putStrLn t
          ) messages
    )
  case e of
    Left err -> throwIO err
    Right () -> pure ()

data Command
  = CommandPing
  | CommandRead Read

data Read = Read
  { id :: !Word64
  , count :: !Word64
  , printIds :: !Bool
  , queue :: !Word32
  }

commandParser :: P.Parser Command
commandParser = P.hsubparser $ mconcat
  [ P.command "ping" $ P.info
      (pure CommandPing)
      (P.progDesc "Ping the server")
  , P.command "read" $ P.info
      (CommandRead <$> readParser)
      (P.progDesc "Read messages from the queue")
  ]

readParser :: P.Parser Read
readParser = Read
  <$> P.option P.auto
      ( P.long "id"
     <> P.short 'i'
     <> P.metavar "ID"
     <> P.value 0
     <> P.help "Starting event identifier"
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
  <*> P.argument P.auto
      ( P.metavar "QUEUE"
     <> P.help "Queue identifier"
      )

lpad :: Int -> [Char] -> [Char]
lpad m xs = L.replicate (m - length ys) '0' ++ ys
  where ys = take m xs
