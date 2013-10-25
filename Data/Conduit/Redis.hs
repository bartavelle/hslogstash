{-| Quick conduit for reading from Redis lists. Not tested much, and probably quite slow.
-}
module Data.Conduit.Redis (redisSource, redisSink) where

import Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.ByteString.Char8 as BS
import Network
import Control.Concurrent.MVar
import Database.Redis hiding (String, decode)
import Control.Monad (void,replicateM, forever)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Data.Either (rights)
import Data.Maybe (catMaybes,fromMaybe,isNothing)
import Control.Exception
import Control.Concurrent hiding (yield)

import Debug.Trace

mp :: Either Reply (Maybe (BS.ByteString, BS.ByteString)) -> Maybe BS.ByteString
mp (Right (Just (_, x))) = Just x
mp (Right Nothing) = Nothing
mp x = trace (show x) Nothing

safePush :: BS.ByteString -> MVar Connection -> ConnectInfo -> (BS.ByteString -> IO ()) -> BS.ByteString -> IO ()
safePush list mconn cinfo logfn input = catch mypush (\SomeException{} -> resetMVar)
    where
        resetMVar = do
            void $ takeMVar mconn
            connect cinfo >>= putMVar mconn
            logfn "reconnecting to redis server ..."
            threadDelay 500000
            safePush list mconn cinfo logfn input
        mypush = do
            conn <- readMVar mconn
            x <- runRedis conn (lpush list [input])
            case x of
                Left (SingleLine "OK") -> return ()
                Right _ -> return ()
                err -> logfn ("retrying ... " `BS.append` BS.pack (show err)) >> threadDelay 500000 >> safePush list mconn cinfo logfn input

popN :: BS.ByteString -> Int -> Integer -> Redis [BS.ByteString]
popN l n to = do
    f <- fmap mp (brpop [l] to)
    if isNothing f
        then return [] -- short circuiting
        else do
            nx <- fmap rights $ replicateM (n-1) (rpop l)
            return $ catMaybes (f:nx)

-- | This is a source that pops elements from a Redis list. It is capable
-- of poping several elements at once, and will return lists of
-- ByteStrings. You might then use 'Data.Conduit.Misc.concat' or the
-- flushing facilities in "Data.Conduit.Misc" to work with individual
-- elements.
redisSource :: (MonadResource m)
            => HostName         -- ^ Hostname of the Redis server
            -> Int              -- ^ Port of the Redis server (usually 6379)
            -> BS.ByteString    -- ^ Name of the list
            -> Int              -- ^ Number of elements to pop at once
            -> Integer          -- ^ Timeout of the brpop function in seconds, useful for getting Flush events into your conduit. Set to 0 for no timeout.
            -> Source m [BS.ByteString]
redisSource h p list nb to =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        myPipe :: (MonadResource m) => Connection -> Source m [BS.ByteString]
        myPipe conn = forever (liftIO (runRedis conn (popN list nb to)) >>= yield)
    in  bracketP (connect cinfo) (\conn -> runRedis conn (void quit)) myPipe

-- | A Sink that will let you write ByteStrings to a redis queue. It can be
-- augmented with a logging function, that will be able to report errors.
redisSink :: (MonadResource m)
          => HostName         -- ^ Hostname of the Redis server
          -> Int              -- ^ Port of the Redis server (usually 6379)
          -> BS.ByteString    -- ^ Name of the list
          -> Maybe (BS.ByteString -> IO ()) -- ^ Command used to log various errors. Defaults to BS.putStrLn. It must not fail, so be careful about exceptions.
          -> Sink BS.ByteString m ()
redisSink h p list logcmd =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        logfunc = fromMaybe BS.putStrLn logcmd
    in  bracketP (connect cinfo >>= newMVar) (const $ return ()) (\mconn -> CL.mapM_ (liftIO . safePush list mconn cinfo logfunc))

