module Main where

import Logstash.Message
import Data.Aeson

import qualified Data.ByteString.Lazy as BS

main :: IO ()
main = fmap (decode :: BS.ByteString -> Maybe LogstashMessage) BS.getContents >>= print
