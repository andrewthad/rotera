{-# language BangPatterns #-}
{-# language ScopedTypeVariables #-}
{-# language OverloadedStrings #-}
{-# language LambdaCase #-}

import Prelude hiding (read)
import Rotera (Settings(..),new,push,read)
import Test.Tasty (defaultMain,testGroup)
import Test.Tasty.HUnit (testCase,assertBool,(@?=))
import Data.Word (Word8)
import Data.Char (ord)
import Control.Monad (forM_)
import Control.Exception (SomeException,try)
import GHC.Exts (RealWorld)
import Data.Foldable (foldlM)
import Control.Concurrent (forkIO)
import System.Random (randomRIO)
import Data.ByteString (ByteString)

import qualified Data.IORef as IO
import qualified Data.ByteString.Char8 as BC
import qualified Data.Set as S
import qualified Control.Concurrent as C
import qualified Data.ByteString as B
import qualified System.Directory as DIR
import qualified Data.Primitive.MVar as PM

main :: IO ()
main = defaultMain $ testGroup "rotera"
  [ testGroup "sequential"
    [ testCase "a" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r "hello world"
        x <- read r 0
        x @?= (0,"hello world")
    , testCase "b" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r "hello"
        push r "world"
        x <- read r 0
        y <- read r 1
        x @?= (0,"hello")
        y @?= (1,"world")
    , testCase "c" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.replicate 1000 (c2w 'a'))
        push r (B.replicate 1000 (c2w 'b'))
        push r (B.replicate 1000 (c2w 'c'))
        push r (B.replicate 1000 (c2w 'd'))
        w <- read r 0
        x <- read r 1
        y <- read r 2
        z <- read r 3
        w @?= (0,B.replicate 1000 (c2w 'a'))
        x @?= (1,B.replicate 1000 (c2w 'b'))
        y @?= (2,B.replicate 1000 (c2w 'c'))
        z @?= (3,B.replicate 1000 (c2w 'd'))
    , testCase "d" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.replicate 1000 (c2w 'a'))
        push r (B.replicate 1000 (c2w 'b'))
        push r (B.replicate 1000 (c2w 'c'))
        push r (B.replicate 1000 (c2w 'd'))
        push r (B.replicate 1000 (c2w 'e'))
        w <- read r 0
        x <- read r 4
        y <- read r 5
        w @?= (3,B.replicate 1000 (c2w 'd'))
        x @?= (4,B.replicate 1000 (c2w 'e'))
        y @?= (4,B.replicate 1000 (c2w 'e'))
    , testCase "e" $ do
        let start = 470 :: Int
        DIR.removePathForcibly "example.bin"
        r <- new settings
        forM_ (enumFromTo start (512 :: Int)) $ \i -> do
          let bytes = B.replicate i (fromIntegral i)
          push r bytes
          w <- read r (i - start)
          w @?= (i - start,bytes)
    ]
  , testGroup "parallel"
    [ testCase "a" $ do
        let threads = 40
        DIR.removePathForcibly "example.bin"
        r <- new settings
        sem <- C.newQSemN 0
        doneWriting <- IO.newIORef False
        results <- foldCommuteIO id
          ( let go1 c = if c <= 'z'
                  then do
                    n <- randomRIO (1,threads)
                    C.waitQSemN sem n
                    push r (BC.replicate 800 c)
                    C.signalQSemN sem n
                    go1 (succ c)
                  else IO.writeIORef doneWriting True
             in (go1 'a' *> pure S.empty)
          : replicate threads
            ( do -- Signal immidiately when the thread starts. The writer thread
                 -- starts before any of the readers, but it has to wait for
                 -- several of the readers to start before it actually gets a
                 -- chance to push anything into the queue. This makes it
                 -- unfathomably unlikely that the writer runs to completion
                 -- before any of the readers get a chance to do any work.
                 C.signalQSemN sem 1
                 let go2 !xs = IO.readIORef doneWriting >>= \case
                       False -> do
                         n <- randomRIO (1,threads)
                         C.waitQSemN sem n
                         -- Always read the earliest possible element on the
                         -- queue. The goal here is to ensure that the writer
                         -- thread does not corrupt our reads by purging old
                         -- data while it is still being read.
                         (ident', bytes) <- read r (-1)
                         C.signalQSemN sem n
                         go2 (if ident' == (-1) then xs else bytes : xs)
                       True -> pure (S.singleton xs)
                 go2 []
            )
          )
        -- The boring assertion is included just to ensure that the test actually
        -- works. It helps mitigate the fear that the writer someone completed
        -- without the readers getting a chance to to anything.
        assertBool "rotera parallel boring" (S.size results > 2)
        -- The dedupe assertion checks that elements in the queue were not
        -- corrupted.
        (dedupedResults :: [[Char]]) <- maybe (fail "rotera parallel dedupe") pure (mapM (mapM (dedupe 800)) (S.toList results))
        -- The missing assertion checks that at some point, a reader actually got
        -- behind enough to missing an event in the queue.
        let z = all isHighToLow dedupedResults
        -- The primary assertion checks that nothing was received out of order. Readers
        -- are allowed to be missing values though (this happens when they get too
        -- far behind the writer). It also checks that at some point, every reader
        -- actually got behind enough to missing an event in the queue. It also checks
        -- that at some point, every reader must do a duplicate read. Statistically,
        -- this must happen.
        assertBool "rotera parallel primary" z
    ]
  ]

settings :: Settings
settings = Settings 4096 1024 2 "example.bin"

c2w :: Char -> Word8
c2w = fromIntegral . ord

-- This takes fixed-length bytestrings of the form AAA...AAA and extracts
-- the single repeated character.
dedupe ::
     Int -- expected number of bytes per bytestring
  -> ByteString
  -> Maybe Char
dedupe n bs = if n > 0 && B.length bs == n && B.minimum bs == B.maximum bs
  then Just (BC.head bs)
  else Nothing

-- This checks that the sequence is is descending order, that there
-- is at least one skip in the sequence, and that there is at least
-- one duplication in the sequence. These succeed:
--   GFDDCBA
--   GFBAA
--   GAAAA
-- These do not succeed:
--   DCBA
--   CDDDA
--   FDA
isHighToLow :: (Ord a, Enum a) => [a] -> Bool
isHighToLow [] = True
isHighToLow (x : xs) = isHighToLow' False False x xs

isHighToLow' :: (Ord a, Enum a) => Bool -> Bool -> a -> [a] -> Bool
isHighToLow' hasNonadjacent hasDuplication _ [] = hasNonadjacent && hasDuplication
isHighToLow' hasNonadjacent hasDuplication x (y : ys) = x >= y && isHighToLow' ((x /= succ y && x /= y) || hasNonadjacent) (x == y || hasDuplication) y ys

-- A more performant specialization of 'foldMapM' that is only valid
-- for commutative monoids. Throws an error upon an exception.
foldCommuteIO :: forall t m a. (Foldable t, Monoid m) => (a -> IO m) -> t a -> IO m
foldCommuteIO f xs = do
  (var :: PM.MVar RealWorld (Either SomeException m)) <- PM.newEmptyMVar
  total <- foldlM (\ !n a -> forkIO (try (f a) >>= PM.putMVar var) *> pure (n + 1)) 0 xs 
  let go2 :: Int -> SomeException -> IO (Either SomeException m)
      go2 !n e = if (n :: Int) < total
        then PM.takeMVar var *> go2 (n + 1) e
        else pure $ Left e
  let go :: Int -> m -> IO (Either SomeException m)
      go !n !m = if (n :: Int) < total
        then PM.takeMVar var >>= \case
          Left r -> go2 (n + 1) r
          Right m' -> go (n + 1) (m <> m')
        else pure $ Right m
  x <- go 0 mempty
  case x of
    Left e -> error $ "Exception encountered in foldCommuteIO thread. Terminating: " <> show e
    Right m -> pure m

