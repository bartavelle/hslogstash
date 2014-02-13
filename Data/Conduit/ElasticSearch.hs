{-# LANGUAGE RankNTypes #-}
{-| This module exports "Conduit" interfaces to ElasticSearch.
It has been used intensively in production for several month now, but at a single site.
-}
module Data.Conduit.ElasticSearch (esConduit, esSearchSource, esScan) where

import Data.Maybe (fromMaybe)
import Control.Exception
import Data.Conduit
import qualified Data.Conduit.List as CL
import Network.HTTP.Conduit
import Data.Aeson
import Control.Applicative
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.Time
import qualified Data.Text.Lazy.Encoding as E
import Data.Text.Format (format,left)
import Logstash.Message
import Control.Monad
import Control.Monad.IO.Class
import Control.Concurrent (threadDelay)
import Data.Either (partitionEithers)
import Data.Monoid
import Network.HTTP.Types
import Control.Lens hiding ((.=))
import Data.Aeson.Lens
import Data.Default

import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

data SearchObject = SearchObject { _source :: LogstashMessage
                                 , _index  :: T.Text
                                 , _score  :: Double
                                 , _id     :: T.Text
                                 , _type   :: T.Text
                                 } deriving (Show)

instance FromJSON SearchObject where
    parseJSON (Object v) = SearchObject
                            <$> v .: "_source"
                            <*> v .: "_index"
                            <*> v .: "_score"
                            <*> v .: "_id"
                            <*> v .: "_type"
    parseJSON _          = mzero

data Hits = Hits { _rhits :: [SearchObject]
                 , _maxScore :: Double
                 , _total :: Int
                 } deriving (Show)

instance FromJSON Hits where
    parseJSON (Object v) = Hits
                            <$> v .: "hits"
                            <*> v .: "max_score"
                            <*> v .: "total"
    parseJSON _          = mzero

data SearchResponse = SearchResponse { _timedOut :: Bool
                                     , _hits :: Hits
                                     , _took :: Integer
                                     } deriving (Show)

instance FromJSON SearchResponse where
    parseJSON (Object v) = SearchResponse
                            <$> v .: "timed_out"
                            <*> v .: "hits"
                            <*> v .: "took"
    parseJSON _          = mzero

-- | A source of Logstash messages generated from an ElasticSearch query.
esSearchSource :: (MonadResource m) => Maybe Request -- ^ Defaults parameters for the http request to ElasticSearch. Use "Nothing" for defaults.
               -> BS.ByteString -- ^ Hostname of the ElasticSearch server
               -> Int -- ^ Port of the HTTP interface (usually 9200)
               -> BS.ByteString -- ^ Prefix of the index (usually logstash)
               -> Value -- ^ Request
               -> Int -- ^ Maximum size of each response
               -> Int -- ^ start
               -> Producer m (Either Value [LogstashMessage])
esSearchSource r h p prefix req maxsize start = self start
    where
        defR1 = fromMaybe def r
        defR2 = defR1 { host = h
                      , port = p
                      , path = prefix <> "/_search"
                      , method = "POST"
                      , checkStatus = \_ _ _ -> Nothing
                      }
        self :: (MonadResource m) => Int -> Producer m (Either Value [LogstashMessage])
        self i = do
            let body = encode $ object [ ("query", req), "from" .= i, "size" .= maxsize ]
                request = defR2 { requestBody = RequestBodyLBS body }
            resp <- liftIO (safeQuery request)
            let respbody = decode (responseBody resp) :: Maybe SearchResponse
                errbody = fromMaybe Null (decode (responseBody resp)) :: Value
                code = statusCode $ responseStatus resp
            case (code, respbody, errbody) of
                (200, Just x, _) -> do
                    let hits = _hits x
                        total = _total hits
                        messages = map _source (_rhits hits)
                        nexti = i + maxsize
                    yield (Right messages)
                    when (nexti < total) (self nexti)
                _ -> yield (Left errbody)

-- | Use this function for \'scanning\' requests, using the scroll feature.
esScan :: Maybe Request -- ^ Defaults parameters for the http request to ElasticSearch. Use "Nothing" for defaults.
               -> BS.ByteString -- ^ Hostname of the ElasticSearch server
               -> Int -- ^ Port of the HTTP interface (usually 9200)
               -> BS.ByteString -- ^ Name of the index (Something like logstash-2013.12.06)
               -> Int -- ^ Maximum size of each response
               -> Producer (ResourceT IO) (Either Value [Value])
esScan r h p indx maxsize = do
    resp <- liftIO (safeQuery defRScan)
    let respbody = decode (responseBody resp) :: Maybe Value
        code = statusCode $ responseStatus resp
    case (code, respbody ^? _Just . key "_scroll_id") of
        (200, Just (String scrid)) -> self scrid
        _ -> yield (Left (fromMaybe "could not parse json" respbody))
    where
        defR1 = fromMaybe def r
        defRScan = defR1 { host = h
                         , port = p
                         , path = indx <> "/_search"
                         , queryString = "search_type=scan&scroll=5m&size=" <> BS.pack (show maxsize)
                         , method = "GET"
                         , checkStatus = \_ _ _ -> Nothing
                         }
        self :: (MonadResource m) => T.Text -> Producer m (Either Value [Value])
        self scrollid = do
            let req = defRScan { path = "/_search/scroll"
                               , queryString = "scroll=5m&scroll_id=" <> T.encodeUtf8 scrollid
                               }
            resp <- liftIO (safeQuery req)
            let respbody = decode (responseBody resp) :: Maybe Value
                mscrollid = respbody ^? _Just . key "_scroll_id"
                mvals = respbody ^.. _Just . key "hits" . key "hits" . _Array . traverse . key "_source"
            -- liftIO (print respbody)
            case (mscrollid, mvals) of
                (Just _, []) -> return ()
                (Just (String nscr), hts) -> yield (Right hts) >> self nscr
                _ -> yield (Left (fromMaybe "could not parse json" respbody))

safeQuery :: Request -> IO (Response BSL.ByteString)
safeQuery req = catch (withManager $ httpLbs ureq ) (\e -> print (e :: SomeException) >> threadDelay 500000 >> safeQuery req)
    where
        ureq = req { responseTimeout = Nothing }

-- | Takes a "LogstashMessage", and returns the result of the ElasticSearch request
-- along with the value in case of errors, or ElasticSearch's values in case of
-- success.
esConduit :: (MonadResource m) => Maybe Request -- ^ Defaults parameters for the http request to ElasticSearch. Use "Nothing" for defaults.
            -> BS.ByteString -- ^ Hostname of the ElasticSearch server
            -> Int -- ^ Port of the HTTP interface (usually 9200)
            -> T.Text -- ^ Prefix of the index (usually logstash)
            -> Conduit [LogstashMessage] m [Either (LogstashMessage, Value) Value]
esConduit r h p prefix = CL.map (map prepareBS) =$= CL.mapM sendBulk
    where
        defR1 = fromMaybe def r
        defR2 = defR1 { host = h
                      , port = p
                      , path = "/_bulk"
                      , method = "POST"
                      , checkStatus = (\_ _ _ -> Nothing)
                      }
        prepareBS :: LogstashMessage -> Either (LogstashMessage, Value) (LogstashMessage, Value, Value)
        prepareBS input =
            case logstashTime input of
                Nothing -> Left (input, object [ "error" .= String "Time was not supplied" ])
                Just (UTCTime day _) ->
                    let (y,m,d) = toGregorian day
                        indx = format "{}-{}.{}.{}" (prefix, y, left 2 '0' m, left 2 '0' d)
                    in  Right (input, object [ "index" .= object [ "_index" .= indx, "_type" .= logstashType input ] ], toJSON input)
        sendBulk :: (MonadResource m) => [Either (LogstashMessage, Value) (LogstashMessage, Value, Value)] -> m [Either (LogstashMessage, Value) Value]
        sendBulk input =
            let (errors, tosend) = partitionEithers input
                lerrors = map Left errors
                body = BSL.unlines $ concatMap (\(_,x,y) -> [encode x, encode y]) tosend
                req = defR2 { requestBody = RequestBodyLBS body }
            in do
                res' <- liftIO (safeQuery req)
                let genericError er = return (Left (emptyLSMessage "error", object [ "error" .= T.pack er, "data" .= res ]) : lerrors)
                    res = case E.decodeUtf8' (responseBody res') of
                              Right x -> x ^. strict
                              Left  _ -> "Error, would not decode"
                    getObject st (Object hh) = HM.lookup st hh
                    getObject _ _ = Nothing
                    fst3 (a,_,_) = a
                    items = getObject "items" (String res)
                    extractErrors x = if (getObject "create" (snd x) >>= getObject "ok") == Just (Bool True)
                                        then Right (snd x)
                                        else Left x
                case items of
                    Just (Array v) -> return $ map extractErrors (zip (map fst3 tosend) (V.toList v)) ++ lerrors
                    _ -> genericError "Can't find items"

