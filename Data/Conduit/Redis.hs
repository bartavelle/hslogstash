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
import Data.Maybe (catMaybes)
import Control.Exception
import Control.Concurrent hiding (yield)

mp :: Either Reply (Maybe (BS.ByteString, BS.ByteString)) -> Maybe BS.ByteString
mp (Right (Just (_, x))) = Just x
mp _ = Nothing

safePush :: BS.ByteString -> MVar Connection -> ConnectInfo -> BS.ByteString -> IO ()
safePush list mconn cinfo input = catch mypush (\SomeException{} -> resetMVar)
    where
        resetMVar = do
            void $ takeMVar mconn
            connect cinfo >>= putMVar mconn
            BS.putStrLn "reconnecting to redis server ..."
            threadDelay 500000
            safePush list mconn cinfo input
        mypush = do
            conn <- readMVar mconn
            x <- runRedis conn (lpush list [input])
            case x of
                Left (SingleLine "OK") -> return ()
                Right _ -> return ()
                err -> BS.putStrLn ("retrying ... " `BS.append` BS.pack (show err)) >> threadDelay 500000 >> safePush list mconn cinfo input

popN :: BS.ByteString -> Int -> Redis [BS.ByteString]
popN l n = do
    f <- fmap mp (brpop [l] 0)
    nx <- fmap rights $ replicateM (n-1) (rpop l)
    return $ catMaybes (f:nx)

redisSource :: (MonadResource m)
            => HostName         -- ^ Hostname of the Redis server
            -> Int              -- ^ Port of the Redis server (usually 6379)
            -> BS.ByteString    -- ^ Name of the list
            -> Int              -- ^ Number of elements to pop at once
            -> Source m [BS.ByteString]
redisSource h p list nb =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        myPipe :: (MonadResource m) => Connection -> Source m [BS.ByteString]
        myPipe conn = forever (liftIO (runRedis conn (popN list nb)) >>= yield)
    in  bracketP (connect cinfo) (\conn -> runRedis conn (void quit)) myPipe

-- | Warning, this outputs strings when things go wrong!
redisSink :: (MonadResource m)
          => HostName         -- ^ Hostname of the Redis server
          -> Int              -- ^ Port of the Redis server (usually 6379)
          -> BS.ByteString    -- ^ Name of the list
          -> Sink BS.ByteString m ()
redisSink h p list =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
    in  bracketP (connect cinfo >>= newMVar) (const $ return ()) (\mconn -> CL.mapM_ (liftIO . safePush list mconn cinfo))

