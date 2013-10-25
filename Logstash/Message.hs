-- |  Datatypes, helper functions, and JSON instances for Logstash
-- messages.
module Logstash.Message where

import Data.Aeson
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Control.Applicative
import Control.Monad
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import Data.Time
import Data.Text.Format

import Data.Attoparsec.Text

{-| The Logstash message, as described in <https://github.com/logstash/logstash/wiki/logstash's-internal-message-format>.
Please note that it is good practice to forget about the timestamp when creating messages (set 'logstashTime' to 'Nothing'), as it should be a responsability of the Logstash server to add it.
-}
data LogstashMessage = LogstashMessage
                     { logstashType    :: T.Text
                     , logstashSource  :: T.Text
                     , logstashTags    :: [T.Text]
                     , logstashFields  :: Value
                     , logstashContent :: T.Text
                     , logstashTime    :: Maybe UTCTime
                     } deriving (Show, Eq)

instance FromJSON LogstashMessage where
    parseJSON (Object v) = LogstashMessage
                        <$> v .:  "@type"
                        <*> v .:  "@source"
                        <*> v .:  "@tags"
                        <*> v .:  "@fields"
                        <*> v .:  "@message"
                        <*> v .:? "@timestamp"
    parseJSON _          = mzero

{-| As the name implies, this creates a dummy Logstash message, only
updating the message field.
-}
emptyLSMessage :: T.Text -> LogstashMessage
emptyLSMessage m = LogstashMessage "empty" "dummy" [] (object []) m Nothing

instance ToJSON LogstashMessage where
    toJSON (LogstashMessage ty s ta f c ts) = object $ [ "@type"    .= ty
                                                       , "@source"  .= s
                                                       , "@tags"    .= ta
                                                       , "@fields"  .= f
                                                       , "@message" .= c
                                                       ] ++ case ts of
                                                                Nothing -> []
                                                                Just  t -> [ "@timestamp" .= t ]

-- | This formats an UTCTime in what logstash expects
logstashTimestamp :: UTCTime -> T.Text
logstashTimestamp (UTCTime d t) = TL.toStrict $! format "{}-{}-{}T{}:{}:{}.{}Z" (year, tc month, tc day, tc hours, tc minutes, tc seconds, left 3 '0' imicro)
    where
        tc = left 2 '0'
        reduce :: Int -> Int -> (Int, Int)
        reduce a b = (a `mod` b, a `div` b)
        (year, month, day) = toGregorian d
        (fseconds, micro)  = properFraction t
        imicro = truncate (micro * 1000) :: Int
        (seconds, fminutes) = reduce fseconds 60
        (minutes, hours)    = reduce fminutes 60

-- | This parses the logstash time format.
parseLogstashTime :: T.Text -> Maybe UTCTime
parseLogstashTime t = case parseOnly prs t of
                          Right r -> Just r
                          Left _  -> Nothing
    where
        prs = do
            ye <- decimal <* char '-' :: Parser Integer
            mo <- decimal <* char '-' :: Parser Int
            da <- decimal <* char 'T' :: Parser Int
            ho <- decimal <* char ':' :: Parser Int
            mi <- decimal <* char ':' :: Parser Int
            se <- decimal <* char '.' :: Parser Int
            ms <- decimal <* char 'Z' :: Parser Int
            endOfInput
            let !seconds = ho*3600 + mi*60 + se
                !micro   = fromIntegral ms / 1000
                !secs    = secondsToDiffTime (fromIntegral seconds) + micro
            return $! UTCTime (fromGregorian ye mo da) secs

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
                    _ -> Nothing
        mmsg  = case HM.lookup "@message" m of
                    Just (String x) -> x
                    _ -> ""
        mts   = case HM.lookup "@timestamp" m of
                    Just (String u) -> parseLogstashTime u
                    _ -> Nothing
        toTags :: [Value] -> Maybe [T.Text]
        toTags v =
            let isString (String _) = True
                isString _ = False
                toText (String x) = x
                toText _ = ""
            in  if null (filter (not . isString) v)
                    then Just (map toText v)
                    else Nothing
    in case (mtype, msrc, mtags) of
           (Just (String t), Just (String s), Just tags) -> Just $ LogstashMessage t s tags mflds mmsg mts
           _ -> Nothing
value2logstash _ = Nothing

-- | Adds the current timestamp if it is not provided.
addLogstashTime :: LogstashMessage -> IO LogstashMessage
addLogstashTime msg = case logstashTime msg of
                          Just _  -> return msg
                          Nothing -> do
                              curtime <- getCurrentTime
                              return msg { logstashTime = Just curtime }

-- | Adds a tag to a logstash message.
addLogstashTag :: T.Text -- ^ The tag to add
               -> LogstashMessage
               -> LogstashMessage
addLogstashTag tag msg = msg { logstashTags = tag : logstashTags msg }

