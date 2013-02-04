{-| This module exports "Conduit" interfaces to ElasticSearch. It is
totally experimental.
-}
module Data.Conduit.ElasticSearch (esConduit) where

import Prelude hiding (catch)
import Control.Exception
import Data.Conduit
import qualified Data.Conduit.List as CL
import Network.HTTP.Conduit
import Data.Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.Time
import qualified Data.Text.Lazy.Encoding as E
import Data.Text.Format (format,left)
import Logstash.Message
import Control.Monad.IO.Class
import Control.Concurrent (threadDelay)
import Data.Either (partitionEithers)

import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import qualified Data.Text as T

safeQuery :: Request (ResourceT IO) -> IO (Response BSL.ByteString)
safeQuery req = catch (withManager $ httpLbs req) (\e -> print (e :: SomeException) >> threadDelay 500000 >> safeQuery req)

-- | Takes a "LogstashMessage", and returns the result of the ES request
-- along with the value in case of errors, or ES's values in case of
-- success
esConduit :: (MonadResource m) => Maybe (Request m) -- ^ Defaults parameters for the http request to ElasticSearch. Use "Nothing" for defaults.
            -> BS.ByteString -- ^ Hostname of the ElasticSearch server
            -> Int -- ^ Port of the HTTP interface (usually 9200)
            -> Conduit [LogstashMessage] m [Either (LogstashMessage, Value) Value]
esConduit r h p = CL.map (map prepareBS) =$= CL.mapM sendBulk
    where
        defR1 = case r of
                    Just x -> x
                    Nothing -> def
        defR2 = defR1 { host = h
                      , port = p
                      , path = "/_bulk"
                      , method = "POST"
                      , checkStatus = (\_ _ -> Nothing)
                      }
        prepareBS :: LogstashMessage -> Either (LogstashMessage, Value) (LogstashMessage, Value, Value)
        prepareBS input =
            case logstashTime input of
                Nothing -> Left (input, object [ "error" .= String "Time was not supplied" ])
                Just (UTCTime day _) ->
                    let (y,m,d) = toGregorian day
                        index = BSL.toStrict (E.encodeUtf8 (format "logstash-{}.{}.{}" (y, left 2 '0' m, left 2 '0' d)))
                    in  Right (input, object [ "index" .= object [ "_index" .= index, "_type" .= logstashType input ] ], toJSON input)
        sendBulk :: (MonadResource m) => [Either (LogstashMessage, Value) (LogstashMessage, Value, Value)] -> m [Either (LogstashMessage, Value) Value]
        sendBulk input =
            let (errors, tosend) = partitionEithers input
                lerrors = map Left errors
                body = BSL.unlines $ concatMap (\(_,x,y) -> [encode x, encode y]) tosend
                req = defR2 { requestBody = RequestBodyLBS body }
            in do
                res <- fmap (responseBody) $ liftIO $ safeQuery req
                let genericError er = return (Left (emptyLSMessage "error", object [ "error" .= (T.pack er), "data" .= res ]) : lerrors)
                    getObject st (Object hh) = HM.lookup st hh
                    getObject _ _ = Nothing
                    fst3 (a,_,_) = a
                    items = decode res >>= getObject "items"
                    extractErrors x = if (getObject "create" (snd x) >>= getObject "ok") == Just (Bool True)
                                        then Right (snd x)
                                        else Left x
                case items of
                    Just (Array v) -> return $ map extractErrors (zip (map fst3 tosend) (V.toList v)) ++ lerrors
                    _ -> genericError "Can't find items"
