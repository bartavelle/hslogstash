{-| Quick conduit for reading from Redis lists. Not tested much, and probably quite slow.
-}
module Data.Conduit.Redis (redisSource) where

import Data.Conduit
import qualified Data.ByteString.Char8 as BS
import Network
import Database.Redis hiding (String, decode)
import Control.Monad (void,replicateM, forever)
import Control.Monad.IO.Class (liftIO)
import Data.Either (rights)
import Data.Maybe (catMaybes)

mp :: Either Reply (Maybe (BS.ByteString, BS.ByteString)) -> Maybe BS.ByteString
mp (Right (Just (_, x))) = Just x
mp _ = Nothing

popN :: BS.ByteString -> Int -> Redis [BS.ByteString]
popN l n = do
    f <- fmap mp (brpop [l] 0)
    nx <- fmap rights $ replicateM (n-1) (rpop l)
    return $ catMaybes (f:nx)

redisSource :: (MonadResource m) => HostName -- ^ Hostname of the Redis server
                -> Int -- ^ Port of the Redis server (usually 6379)
                -> BS.ByteString -- ^ Name of the list
                -> Int -- ^ Number of elements to pop at once
                -> Source m [BS.ByteString]
redisSource h p list nb =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        myPipe :: (MonadResource m) => Connection -> Source m [BS.ByteString]
        myPipe conn = forever (liftIO (runRedis conn (popN list nb)) >>= yield)
    in  bracketP (connect cinfo) (\conn -> runRedis conn (void quit)) myPipe
