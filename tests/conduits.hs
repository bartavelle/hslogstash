module Main where

import Test.Hspec
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Conduit.Misc as CM
import Test.QuickCheck
import Data.Functor.Identity
import Control.Monad.IO.Class
import Data.Conduit.Branching
import Control.Concurrent.STM

main :: IO ()
main = hspec $ do
    describe "concat" $ do
        it "should work just like Prelude.concat" $ do
            property $ \x ->
                let y = runIdentity $ CL.sourceList (x :: [[Int]]) $= CM.concat $$ CL.consume
                in y == Prelude.concat x
    describe "simpleConcatFlush" $ do
        it "should separate a list when max = 1" $ do
            x <- CL.sourceList [ [1,2,3,4,5,6] :: [Int] ] $= simpleConcatFlush 1 $= groupFlush $$ CL.consume
            x `shouldBe` map return [1,2,3,4,5,6]
        it "should pair values" $ do
            x <- CL.sourceList [ [1,2,3,4,5,6] :: [Int] ] $= simpleConcatFlush 2 $= groupFlush $$ CL.consume
            x `shouldBe` [ [1,2] , [3,4] , [5,6] ]
        it "should not alter small lists" $ do
            x <- CL.sourceList [ [1,2,3,4,5,6] :: [Int] ] $= simpleConcatFlush 10 $= groupFlush $$ CL.consume
            x `shouldBe` [ [1,2,3,4,5,6] ]
        it "should correctly group values" $ do
            x <- CL.sourceList [ [1,2] , [3,4,5,6], [7,8,9] :: [Int] ] $= simpleConcatFlush 4 $= groupFlush $$ CL.consume
            x `shouldBe` [ [1,2,3,4],[5,6,7,8],[9] ]
        it "should flush on empty input" $ do
            x <- CL.sourceList [ [1,2] , [], [3,4,5], [6], [7,8,9], [] :: [Int] ] $= simpleConcatFlush 4 $= groupFlush $$ CL.consume
            x `shouldBe` [ [1,2], [3,4,5,6],[7,8,9] ]
    describe "concatFlushSum" $ do
        it "should separate input at the right place" $ do
            x <- CL.sourceList [ [1,2,0,0,0,6,3,2,0,0,1] :: [Int] ] $= concatFlushSum id 3 $= groupFlush $$ CL.consume
            x `shouldBe` [[1,2,0,0,0],[6],[3],[2,0,0,1]]
    describe "branching" $ do
        it "should branch simple eithers" $ do
            tsink0 <- newTVarIO []
            tsink1 <- newTVarIO []
            tsink2 <- newTVarIO []
            let lst = [Left 5, Left 4, Right "five", Right "four"] :: [Either Int String]
                source = CL.sourceList lst
                sink0 = CL.mapM_ (\(Left x)  -> liftIO $ atomically $ modifyTVar tsink0 (\t -> x : t))
                sink1 = CL.mapM_ (\(Right x) -> liftIO $ atomically $ modifyTVar tsink1 (\t -> x : t))
                sink2 = CL.mapM_ (\x -> liftIO $ atomically $ modifyTVar tsink2 (\t -> x : t))
                branching (Left _) = [0,2]
                branching (Right _) = [1,2]
            branchConduits source branching [sink0, sink1, sink2]
            o0 <- atomically (readTVar tsink0)
            o1 <- atomically (readTVar tsink1)
            o2 <- atomically (readTVar tsink2)
            (o0,o1,o2) `shouldBe` (reverse [5,4],reverse ["five","four"],reverse lst)
