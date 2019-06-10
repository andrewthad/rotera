{-# language BangPatterns #-}
{-# language ScopedTypeVariables #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar,takeMVar,putMVar)
import Control.Concurrent.MVar (newEmptyMVar)
import Control.Exception
import Control.Monad (when)
import Data.Word (Word16, Word32)
import Rotera (Rotera(..))
import System.Directory (listDirectory)
import Text.Read (readMaybe)

import qualified Control.Concurrent.STM as STM
import qualified Control.Exception as E
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text as T
import qualified Foreign.Storable as S
import qualified GHC.Exts as E
import qualified Options.Applicative as P
import qualified Rotera as Rotera
import qualified Rotera.Socket as R

main :: IO ()
main = do
  port <- P.execParser $ P.info
    (portParser P.<**> P.helper)
    P.fullDesc

  when (S.sizeOf (undefined :: Int) /= 8) $ do
    E.throwIO (E.AssertionFailed "Not on a 64-bit platform.")

  !intr <- STM.newTVarIO False
  !done <- newEmptyMVar :: IO (MVar ())
  paths <- listDirectory "."
  xs :: [(Word32,Rotera.Rotera)] <- traverse
    ( \file -> do
      let (name,ext) = T.breakOn (T.singleton '.') (T.pack file)
      when (ext /= T.pack ".rot") $ do
        fail ("Encountered file without .rot extension: " ++ file)
      case readMaybe (T.unpack name) of
        Nothing -> fail ("Encountered file with non-numeric name: " ++ file)
        Just (queue :: Word32) -> do
          r@Rotera{deadZoneEvents,maxEventBytes,maximumEvents} <- Rotera.open file
          BC.putStr $ BC.concat
            [ "[localhost][info] Queue "
            , BC.pack (show queue)
            , " can persist "
            , BC.pack (show maxEventBytes)
            , " bytes of messages.\n"
            ]
          BC.putStr $ BC.concat
            [ "[localhost][info] Queue "
            , BC.pack (show queue)
            , " can persist "
            , BC.pack (show maximumEvents)
            , " messages.\n"
            ]
          BC.putStr $ BC.concat
            [ "[localhost][info] Queue "
            , BC.pack (show queue)
            , " reclaims at least "
            , BC.pack (show deadZoneEvents)
            , " messages when space is insufficient.\n"
            ]
          BC.putStr $ BC.concat
            [ "[localhost][info] Queue "
            , BC.pack (show queue)
            , " rejects messages larger than "
            , BC.pack (show (div maxEventBytes (deadZoneEvents + 2)))
            , " bytes.\n"
            ]
          pure (queue,r)
    ) paths
  let resolver = E.fromList (map fst xs)
  let roteras = E.fromList (map snd xs)
  _ <- forkIO $ do
    R.server port intr resolver roteras
    putMVar done ()
  catch
    (takeMVar done)
    (\case
      UserInterrupt -> do
        STM.atomically (STM.writeTVar intr True)
        takeMVar done
      e -> throwIO e
    )

portParser :: P.Parser Word16
portParser = P.option P.auto
  ( P.long "port"
 <> P.short 'p'
 <> P.metavar "WORD16"
 <> P.value 8245
 <> P.showDefault
 <> P.help "Port where rotera-server is running"
  )

