{-# language BangPatterns #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}

import Prelude hiding (id,Read)
import Rotera (Settings(..))
import Options.Applicative ((<**>))
import System.IO (stdin)
import Data.Word (Word32)

import qualified Data.ByteString as B
import qualified Data.Bytes.Mutable as MB
import qualified Data.Bytes.Unsliced as BU
import qualified Rotera as R
import qualified Rotera.Nonblocking as RNB
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Options.Applicative as P
import qualified Data.Text.IO as TIO
import qualified GHC.OldList as L
import qualified Data.Vector.Primitive as PV
import qualified Data.Text.Encoding as TE
import qualified Data.Text as T

main :: IO ()
main = do
  cmd <- P.execParser $ P.info
    (commandParser <**> P.helper)
    P.fullDesc
  run cmd

run :: Command -> IO ()
run = \case
  CommandRead (Read{id,count,printIds}) -> do
    r <- R.open defPath
    (startId,msgs) <- RNB.readMany r id count
    PM.itraverseUnliftedArray_
      ( \ix msg -> do
        let prefix = if printIds
              then T.pack (lpad 9 (show (startId + ix)) ++ " ")
              else T.empty
        TIO.putStr prefix
        case TE.decodeUtf8' (BU.toByteString msg) of
          Left _ -> TIO.putStr "<non-utf8>\n"
          Right t -> TIO.putStrLn t
      ) msgs
  CommandCreate s -> do
    _ <- R.new s defPath
    pure ()
  CommandPush -> do
    r <- R.open defPath
    strs <- fmap (L.filter (not . T.null) . L.map T.strip . T.splitOn (T.singleton '\n')) (TIO.hGetContents stdin)
    let bstrs = L.map TE.encodeUtf8 strs
        lens = L.map ((fromIntegral :: Int -> Word32) . B.length) bstrs
        bstr = B.concat bstrs
    sizes <- PV.thaw (PV.fromList lens)
    mbstr <- MB.thawByteString bstr
    R.pushMany r sizes mbstr
    R.commit r

data Command
  = CommandCreate Settings
  | CommandPush
  | CommandRead Read

data Read = Read
  { id :: !Int
  , count :: !Int
  , printIds :: !Bool
  }

defPath :: String
defPath = "rotera.bin"

commandParser :: P.Parser Command
commandParser = P.hsubparser $ mconcat
  [ P.command "read" $ P.info
      (CommandRead <$> readParser)
      (P.progDesc "Read messages from the queue")
  , P.command "push" $ P.info
      (pure CommandPush)
      (P.progDesc "Push newline-delimited message from stdin into the queue")
  , P.command "create" $ P.info
      (CommandCreate <$> settingsParser)
      (P.progDesc "Create a rotating queue")
  ]

readParser :: P.Parser Read
readParser = Read
  <$> P.option P.auto
      ( P.long "id"
     <> P.short 'i'
     <> P.metavar "ID"
     <> P.value (-1)
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

settingsParser :: P.Parser Settings
settingsParser = Settings
  <$> P.option P.auto
      ( P.long "size"
     <> P.short 's'
     <> P.metavar "BYTES"
     <> P.value (1024 * 1024)
     <> P.help "Size of data section in bytes"
      )
  <*> P.option P.auto
      ( P.long "entries"
     <> P.short 'e'
     <> P.metavar "INT"
     <> P.value 1024
     <> P.help "Maximum number of events to preserve"
      )
  <*> P.option P.auto
      ( P.long "expirations"
     <> P.short 'x'
     <> P.metavar "INT"
     <> P.value 64
     <> P.help "Batch size of expiration"
      )

lpad :: Int -> [Char] -> [Char]
lpad m xs = L.replicate (m - length ys) '0' ++ ys
  where ys = take m xs
