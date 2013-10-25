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
import Data.List.Split (chunksOf,splitOn)
import Data.Either (lefts,rights)
import Data.Maybe (catMaybes)

main :: IO ()
main = hspec $ do
    describe "concat" $ do
        it "should work just like Prelude.concat" $ property $ \x ->
            let y = runIdentity $ CL.sourceList (x :: [[Int]]) $= CM.concat $$ CL.consume
            in y == Prelude.concat x
    describe "simpleConcatFlush" $ do
        it "should behave like expected for simple lists" $ property $ \x -> do
            chunksize <- suchThat arbitrary (>0)
            let y = runIdentity $ CL.sourceList (x :: [[Int]]) $= simpleConcatFlush chunksize $= groupFlush $$ CL.consume
                toMaybeList [] = [Nothing]
                toMaybeList k = map Just k
                z = concatMap (chunksOf chunksize . catMaybes) $ splitOn [Nothing] $ concatMap toMaybeList x
            return (y == z)
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
        it "should branch simple eithers" $ property $ \lst -> do
            tsink0 <- newTVarIO []
            tsink1 <- newTVarIO []
            tsink2 <- newTVarIO []
            let source = CL.sourceList (lst :: [Either Int String])
                sappend f s = CL.mapM_ (\x -> liftIO $ atomically $ modifyTVar s (f x :))
                sink0 = sappend (\(Left x)  -> x) tsink0
                sink1 = sappend (\(Right x) -> x) tsink1
                sink2 = sappend id                tsink2
                branching (Left _)  = [0,2]
                branching (Right _) = [1,2]
            branchConduits source branching [sink0, sink1, sink2]
            o0 <- atomically (readTVar tsink0)
            o1 <- atomically (readTVar tsink1)
            o2 <- atomically (readTVar tsink2)
            let result = (o0,o1,o2)
                expected = (reverse (lefts lst),reverse (rights lst),reverse lst)
            result `shouldBe` expected
