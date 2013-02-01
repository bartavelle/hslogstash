{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Conduit
import Data.Conduit.Logstash
import Data.Conduit.Redis
import qualified Data.Conduit.List as CL
import System.Environment (getArgs)
import Control.Monad (when)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BSL
import Control.Monad.IO.Class (liftIO)
import Logstash.Message
import Data.Aeson

printErrors :: (MonadResource m) => (Either BS.ByteString LogstashMessage) -> m (Maybe LogstashMessage)
printErrors (Left err)  = liftIO (BS.putStrLn err) >> return Nothing
printErrors (Right msg) = return (Just msg)

main :: IO ()
main = do
    args <- getArgs
    when (null args) (error "Usage: LogstashToRedis listening-port [redis-queue] [redis-host] [redis-port]")
    let lport = read (args !! 0)
        queue = if length args > 1
                    then BS.pack (args !! 1)
                    else "logstash"
        host  = if length args > 2
                    then (args !! 2)
                    else "127.0.0.1"
        port  = if length args > 3
                    then read (args !! 3)
                    else 6379
    logstashListener lport (CL.mapM printErrors =$ CL.mapMaybe id =$ CL.mapM (liftIO . addLogstashTime) =$ CL.map (BSL.toStrict . encode) =$ redisSink host port queue)
