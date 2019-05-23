{-# language LambdaCase #-}
{-# language BangPatterns #-}
{-# language NamedFieldPuns #-}

import Prelude hiding (id,Read)
import Rotera (Settings(..))
import Options.Applicative ((<**>))

import qualified Rotera as R
import qualified Rotera.Nonblocking as RNB
import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Options.Applicative as P

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
    let go !ix = if ix < PM.sizeofUnliftedArray msgs
          then do
            print (PM.indexUnliftedArray msgs ix)
            go (ix + 1)
          else pure ()
    go 0

data Command
  = CommandCreate Settings
  | CommandPush Push
  | CommandRead Read

data Push = Push

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
  <*> pure False
