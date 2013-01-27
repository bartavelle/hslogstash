{-| Quick conduit for reading from Redis lists. Not tested much, and probably quite slow.
-}
module Data.Conduit.Redis where

import Data.Conduit
import qualified Data.ByteString.Char8 as BS
import Network
import Database.Redis hiding (String, decode)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)

redisSource :: (MonadResource m) => HostName -- ^ Hostname of the Redis server
                -> Int -- ^ Port of the Redis server (usually 6379)
                -> BS.ByteString -- ^ Name of the list
                -> Source m BS.ByteString
redisSource h p list =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        myPipe :: (MonadResource m) => Connection -> Source m BS.ByteString
        myPipe conn = do
            o <- liftIO $ runRedis conn (blpop [list] 0)
            case o of
                Right (Just (_,k)) -> yield k
                _ -> return ()
    in  bracketP (connect cinfo) (\conn -> runRedis conn (void quit)) myPipe
