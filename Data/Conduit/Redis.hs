module Data.Conduit.Redis where
{- |
    Quick conduit for reading from redis sources.
-}

import Data.Conduit
import Data.Conduit.Util
import qualified Data.ByteString.Char8 as BS
import Network
import Database.Redis hiding (String, decode)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)

redisSource :: (MonadResource m) => HostName -> Int -> BS.ByteString -> Source m BS.ByteString
redisSource h p list =
    let cinfo = defaultConnectInfo { connectHost = h, connectPort = PortNumber $ fromIntegral p }
        pull = do
            o <- blpop [list] 0
            case o of
                Right (Just (_,k)) -> return k
                _ -> pull
    in  sourceStateIO (connect cinfo)
                      (\conn -> runRedis conn (void quit))
                      (\conn -> do
                          o <- liftIO $ runRedis conn pull
                          return (StateOpen conn o)
                      )

