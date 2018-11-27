{-# language OverloadedStrings #-}

import Prelude hiding (read)
import Rotera (Settings(..),new,push,read)
import Test.Tasty (defaultMain,testGroup)
import Test.Tasty.HUnit (testCase,(@?=))

import qualified System.Directory as DIR

main :: IO ()
main = defaultMain $ testGroup "rotera"
  [ testCase "a" $ do
      DIR.removePathForcibly "example.bin"
      r <- new settings
      push r "hello world"
      x <- read r 0
      x @?= (0,"hello world")
  ]

settings :: Settings
settings = Settings (4096 * 16) 1024 2 "example.bin"

