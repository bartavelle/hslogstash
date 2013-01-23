module Logstash.Message where

import Data.Aeson
import qualified Data.Text as T
import Control.Applicative
import Control.Monad
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V

{-| The Logstash message, as described in <https://github.com/logstash/logstash/wiki/logstash's-internal-message-format>.
Please not there is no timestamp, as the logstash server will add it.
-}
data LogstashMessage = LogstashMessage
                     { logStashType    :: T.Text
                     , logStashSource  :: T.Text
                     , logStashTags    :: [T.Text]
                     , logStashFields  :: Value
                     , logStashContent :: T.Text
                     } deriving (Show)

instance FromJSON LogstashMessage where
    parseJSON (Object v) = LogstashMessage
                        <$> v .: "@type"
                        <*> v .: "@source"
                        <*> v .: "@tags"
                        <*> v .: "@fields"
                        <*> v .: "@message"
    parseJSON _          = mzero

{-| As the name implies, this creates a dummy Logstash message, only
updating the message field.
-}
emptyLSMessage :: T.Text -> LogstashMessage
emptyLSMessage m = LogstashMessage "empty" "dummy" [] (object []) m

instance ToJSON LogstashMessage where
    toJSON (LogstashMessage ty s ta f c) = object [ "@type"    .= ty
                                                  , "@source"  .= s
                                                  , "@tags"    .= ta
                                                  , "@fields"  .= f
                                                  , "@message" .= c
                                                  ]

{-| This will try to convert an arbitrary JSON value into
a "LogstashMessage".
-}
value2logstash :: Value -> Maybe LogstashMessage
value2logstash (Object m) =
    let mtype = HM.lookup "@type"   m
        msrc  = HM.lookup "@source" m
        mflds = case HM.lookup "@fields" m of
                    Just x -> x
                    Nothing -> Null
        mtags = case HM.lookup "@tags" m of
                    Just (Array v) -> toTags (V.toList v)
                    Nothing -> Nothing
        mmsg  = case HM.lookup "@message" m of
                    Just (String x) -> x
                    _ -> ""
        toTags :: [Value] -> Maybe [T.Text]
        toTags v =
            let isString (String _) = True
                isString _ = False
                toText (String x) = x
            in  if null (filter (not . isString) v)
                    then Just (map toText v)
                    else Nothing
    in case (mtype, msrc, mtags) of
           (Just (String t), Just (String s), Just tags) -> Just $ LogstashMessage t s tags mflds mmsg
           _ -> Nothing
