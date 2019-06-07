{-# language BangPatterns #-}
{-# language ScopedTypeVariables #-}
{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language OverloadedStrings #-}

import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar,takeMVar,putMVar)
import Control.Concurrent.MVar (newEmptyMVar)
import Data.Word (Word32)
import Text.Read (readMaybe)
import System.Directory (listDirectory)
import Control.Monad (when)
import Rotera (Rotera(..))
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text as T
import qualified GHC.Exts as E
import qualified Rotera as Rotera
import qualified Rotera.Socket as R
import qualified Control.Concurrent.STM as STM
import qualified GHC.OldList as L
import qualified Foreign.Storable as S
import qualified Control.Exception as E

main :: IO ()
main = do
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
    R.server intr resolver roteras
    putMVar done ()
  catch
    (takeMVar done)
    (\case
      UserInterrupt -> do
        STM.atomically (STM.writeTVar intr True)
        takeMVar done
      e -> throwIO e
    )
