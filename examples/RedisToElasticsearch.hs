{-
    This example takes logstash messages from a redis queue and stores them into elasticsearch.
    It will print to stdout all errors.
-}
module Main where

import System.Environment (getArgs)
import Control.Monad (when)
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Conduit.ElasticSearch
import Data.Conduit.Redis
import Logstash.Message
import Control.Monad.IO.Class (liftIO)
import Data.Aeson
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.Maybe (mapMaybe)
import Data.Conduit.Misc

endsink :: (MonadResource m) => Either (LogstashMessage, Value) Value -> m ()
endsink (Left x) = liftIO (print x)
endsink _ = return ()

main :: IO ()
main = do
    args <- getArgs
    when (length args /= 5) (error "Usage: redis2es redishost redisport redislist eshost esport")
    let [redishost, redisport, redislist, eshost, esport] = args
    runResourceT $ redisSource redishost (read redisport) (BS.pack redislist) 100 1
                    $= concatFlush 100 -- convert to a flush conduit
                    $= mapFlushMaybe (decode . BSL.fromStrict) -- decode the json messages
                    $= groupFlush -- regroup lists
                    $= esConduit Nothing (BS.pack eshost) (read esport) "logstash" -- send to ES
                    $$ CL.mapM_ (mapM_ endsink)
