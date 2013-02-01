{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Conduit
import Data.Conduit.Branching
import qualified Data.Conduit.List as CL
import qualified Data.ByteString.Char8 as BS
import Control.Monad.IO.Class (liftIO)
import Logstash.Message
import Control.Concurrent.ParallelIO

printErrors :: (MonadResource m) => (Either BS.ByteString LogstashMessage) -> m (Maybe LogstashMessage)
printErrors (Left err)  = liftIO (BS.putStrLn err) >> return Nothing
printErrors (Right msg) = return (Just msg)

main :: IO ()
main = do
    let _debugsink   = 0
        _infosink    = 1
        _defaultsink = 2
        brfunc msg | "debug" `elem` logstashTags msg = [_debugsink, _defaultsink]
                   | "info"  `elem` logstashTags msg = [_infosink, _defaultsink]
                   | otherwise                       = [_defaultsink]
    (bsink, [debugsource, infosource, defaultsource]) <- mkBranchingConduit 3 brfunc
    let lsmessages = take 100 [ (emptyLSMessage title) { logstashTags = tags } | title <- concat (repeat ["title1", "title2"]), tags <- concat (repeat [["tag"],["debug"],["various","info"]]) ]
        source         = runResourceT $ CL.sourceList lsmessages $= CL.mapM (liftIO . addLogstashTime) $$ bsink
        debugconduit   = runResourceT $ debugsource   $$ CL.mapM_ (\s -> liftIO (putStr "debug   " >> print s))
        infoconduit    = runResourceT $ infosource    $$ CL.mapM_ (\s -> liftIO (putStr "info    " >> print s))
        defaultconduit = runResourceT $ defaultsource $$ CL.mapM_ (\s -> liftIO (putStr "default " >> print s))
    parallel_ [source, debugconduit, infoconduit, defaultconduit]
