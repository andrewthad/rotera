{-# language BangPatterns #-}
{-# language LambdaCase #-}

import Control.Exception
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar,takeMVar,putMVar)
import Control.Concurrent.MVar (newEmptyMVar)
import qualified Rotera.Socket as R
import qualified Control.Concurrent.STM as STM

main :: IO ()
main = do
  !intr <- STM.newTVarIO False
  !done <- newEmptyMVar :: IO (MVar ())
  _ <- forkIO $ do
    R.server intr "rotera.bin"
    putMVar done ()
  catch
    (takeMVar done)
    (\case
      UserInterrupt -> do
        STM.atomically (STM.writeTVar intr True)
        takeMVar done
      e -> throwIO e
    )
