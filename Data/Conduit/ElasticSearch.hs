module Data.Conduit.ElasticSearch where

import Prelude hiding (catch)
import Control.Exception
import Data.Conduit
import qualified Data.Conduit.List as CL
import Network.HTTP.Conduit
import Data.Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Time
import qualified Data.Text.Lazy.Encoding as E
import Data.Text.Format (format,left)
import Logstash.Message
import Control.Monad.IO.Class
import Control.Concurrent (threadDelay)
import qualified Data.HashMap.Strict as HM

safeQuery :: Request (ResourceT IO) -> IO (Response BSL.ByteString)
safeQuery req = catch (withManager $ httpLbs req) (\e -> print (e :: SomeException) >> threadDelay 500000 >> safeQuery req)

-- | Takes JSONifiable values, and returns the result of the ES request
-- | along with the value in case of errors, or ES's values in case of
-- | success
esSink :: (MonadResource m) => Maybe (Request m) -> BS.ByteString -> Int -> Conduit LogstashMessage m (Either (LogstashMessage, Value) Value)
esSink r h p = CL.mapM doIndexA
    where
        defR1 = case r of
                    Just x -> x
                    Nothing -> def
        defR2 = defR1 { host = h
                      , port = p
                      , method = "POST"
                      , checkStatus = (\_ _ -> Nothing)
                      }
        doIndexA :: (MonadResource m) => LogstashMessage -> m (Either (LogstashMessage, Value) Value)
        doIndexA input =
            case logstashTime input of
                Nothing -> return $! Left (input, object [ "error" .= String "Time was not supplied" ])
                Just (UTCTime day _) -> do
                    let (y,m,d) = toGregorian day
                        req = defR2 { path = BSL.toStrict (E.encodeUtf8 (format "/logstash-{}-{}-{}/{}/" (y, left 2 '0' m, left 2 '0' d, logstashType input)))
                                    , requestBody = RequestBodyLBS (encode input)
                                    }
                    res <- liftIO $ safeQuery req
                    case decode (responseBody res) of
                        Just (Object hh) -> case HM.lookup "ok" hh of
                                                Just (Bool True) -> return $! Right (Object hh)
                                                _ -> return $! Left (input, Object hh)
                        Just j  -> return $! Left (input, j)
                        Nothing -> return $! Left (input, object [ "error" .= String "Could not decode", "content" .= responseBody res ])
