{-# language BangPatterns #-}
{-# language ScopedTypeVariables #-}
{-# language OverloadedStrings #-}
{-# language LambdaCase #-}

import Prelude hiding (read)
import Rotera (Settings(..),new,push,pushMany,commit)
import Rotera.Nonblocking as Nonblock

import Control.Concurrent (forkIO)
import Control.Exception (SomeException,try)
import Control.Monad (forM_)
import Data.ByteString (ByteString)
import Data.Bytes.Types (Bytes(..),MutableBytes(..))
import Data.Char (ord)
import Data.Foldable (foldlM)
import Data.Primitive (MutableByteArray)
import Data.Word (Word32)
import Data.Word (Word8)
import GHC.Exts (RealWorld)
import System.Random (randomRIO)
import Test.Tasty (defaultMain,testGroup)
import Test.Tasty.HUnit (testCase,assertBool,(@?=))

import qualified Data.List as L
import qualified GHC.Exts as E
import qualified Data.IORef as IO
import qualified Data.ByteString.Char8 as BC
import qualified Data.Set as S
import qualified Control.Concurrent as C
import qualified Data.ByteString as B
import qualified System.Directory as DIR
import qualified Data.Primitive.MVar as PM
import qualified Data.Vector.Primitive as PV
import qualified Data.Primitive as PM

main :: IO ()
main = defaultMain $ testGroup "rotera"
  [ testGroup "sequential"
    [ testCase "a" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r "hello world"
        commit r
        x <- Nonblock.read r 0
        x @?= Just (0,"hello world")
    , testCase "b" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r "hello"
        push r "world"
        commit r
        x <- Nonblock.read r 0
        y <- Nonblock.read r 1
        x @?= Just (0,"hello")
        y @?= Just (1,"world")
    , testCase "c" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.replicate 1000 (c2w 'a'))
        push r (B.replicate 1000 (c2w 'b'))
        push r (B.replicate 1000 (c2w 'c'))
        push r (B.replicate 1000 (c2w 'd'))
        commit r
        w <- Nonblock.read r 0
        x <- Nonblock.read r 1
        y <- Nonblock.read r 2
        z <- Nonblock.read r 3
        w @?= Just (0,B.replicate 1000 (c2w 'a'))
        x @?= Just (1,B.replicate 1000 (c2w 'b'))
        y @?= Just (2,B.replicate 1000 (c2w 'c'))
        z @?= Just (3,B.replicate 1000 (c2w 'd'))
    , testCase "d" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r ("u" <> B.replicate 1000 (c2w 'a') <> "u")
        push r ("v" <> B.replicate 1000 (c2w 'b') <> "v")
        push r (B.tail $ B.init $ "w" <> B.replicate 1000 (c2w 'c') <> "w")
        push r (B.tail $ B.init $ "x" <> B.replicate 1000 (c2w 'd') <> "x")
        push r (B.take 1002 $ B.drop 2 $ "yyy" <> B.replicate 1000 (c2w 'e') <> "yyy")
        commit r
        w <- Nonblock.read r 0
        x <- Nonblock.read r 4
        y <- Nonblock.read r 5
        w @?= Just (3,B.replicate 1000 (c2w 'd'))
        x @?= Just (4,"y" <> B.replicate 1000 (c2w 'e') <> "y")
        y @?= Nothing
    , testCase "e" $ do
        let start = 470 :: Int
        DIR.removePathForcibly "example.bin"
        r <- new settings
        forM_ (enumFromTo start (512 :: Int)) $ \i -> do
          let bytes = B.replicate i (fromIntegral i)
          push r bytes
          commit r
          w <- Nonblock.read r (i - start)
          w @?= Just (i - start,bytes)
    , testCase "f" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.take 9 (B.drop 1 "abcdefghijk"))
        push r "world"
        commit r
        x <- Nonblock.read r 0
        y <- Nonblock.read r 1
        x @?= Just (0,"bcdefghij")
        y @?= Just (1,"world")
    , testCase "g" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings { settingsExpiredEntries = 5 }
        let x0 = bytesFromVector $ PV.singleton (c2w 'j')
            x1 = bytesFromVector $ PV.replicate 50 (c2w 'x')
            x2 = bytesFromVector $ PV.replicate 90 (c2w 'y')
            x3 = bytesFromVector $ PV.replicate 30 (c2w 'z')
            x4 = bytesFromVector $ PV.singleton (c2w 'k')
        payloadsUnsliced <- thawBytes (x0 <> x1 <> x2 <> x3 <> x4)
        let payloads = MutableBytes payloadsUnsliced 1 (50 + 90 + 30)
        lens <- PV.thaw (PV.fromList [50,90,30 :: Word32])
        push r (B.replicate 500 (c2w 'a'))
        push r (B.replicate 500 (c2w 'b'))
        push r (B.replicate 500 (c2w 'c'))
        push r (B.replicate 500 (c2w 'd'))
        push r (B.replicate 500 (c2w 'a'))
        push r (B.replicate 500 (c2w 'b'))
        push r (B.replicate 500 (c2w 'c'))
        push r (B.replicate 500 (c2w 'd'))
        pushMany r lens payloads
        push r (B.replicate 40 (c2w 'e'))
        commit r
        x <- Nonblock.read r 8
        x @?= Just (8,B.replicate 50 (c2w 'x'))
        y <- Nonblock.read r 9
        y @?= Just (9,B.replicate 90 (c2w 'y'))
        z <- Nonblock.read r 10
        z @?= Just (10,B.replicate 30 (c2w 'z'))
        k <- Nonblock.read r 11
        k @?= Just (11,B.replicate 40 (c2w 'e'))
    , testCase "h" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.replicate 900 (c2w 'a'))
        push r (B.replicate 900 (c2w 'b'))
        push r (B.replicate 900 (c2w 'c'))
        push r (B.replicate 900 (c2w 'd'))
        push r (B.replicate 900 (c2w 'e'))
        push r (B.replicate 40 (c2w 'f'))
        push r (B.replicate 20 (c2w 'g'))
        commit r
        x <- Nonblock.readMany r 5 3
        x @?= (5,E.fromList
          [ E.fromList (L.replicate 40 (c2w 'f'))
          , E.fromList (L.replicate 20 (c2w 'g'))
          ])
    , testCase "i" $ do
        DIR.removePathForcibly "example.bin"
        r <- new settings
        push r (B.replicate 1000 (c2w 'a'))
        push r (B.replicate 1000 (c2w 'b'))
        push r (B.replicate 1000 (c2w 'c'))
        push r (B.replicate 1000 (c2w 'd'))
        push r (B.replicate 50 (c2w 'f'))
        push r (B.replicate 30 (c2w 'g'))
        push r (B.replicate 90 (c2w 'h'))
        commit r
        x <- Nonblock.readMany r 4 3
        x @?= (4,E.fromList
          [ E.fromList (L.replicate 50 (c2w 'f'))
          , E.fromList (L.replicate 30 (c2w 'g'))
          , E.fromList (L.replicate 90 (c2w 'h'))
          ])
        y <- Nonblock.readMany r 5 3
        y @?= (5,E.fromList
          [ E.fromList (L.replicate 30 (c2w 'g'))
          , E.fromList (L.replicate 90 (c2w 'h'))
          ])
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
                    commit r
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
                         mr <- read r 0
                         C.signalQSemN sem n
                         let ys = case mr of
                               Nothing -> xs
                               Just (_,bytes) -> bytes : xs
                         go2 ys
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

bytesFromVector :: PV.Vector Word8 -> Bytes
bytesFromVector (PV.Vector off len arr) = Bytes arr off len

thawBytes :: Bytes -> IO (MutableByteArray RealWorld)
thawBytes (Bytes arr off len) = do
  marr <- PM.newByteArray len
  PM.copyByteArray marr 0 arr off len
  pure marr

