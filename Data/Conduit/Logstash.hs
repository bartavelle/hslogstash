-- | Receive logstash messages from the network, and process them with
-- a conduit.
module Data.Conduit.Logstash (logstashListener,tryDecode) where

import Data.Conduit
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import Data.Conduit.Network
import Data.Aeson
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString as BS
import Codec.Text.IConv
import Data.Text.Encoding
import Logstash.Message

-- | Decodes JSON data from ByteStrings that can be encoded in UTF-8 or
-- latin1.
tryDecode :: (FromJSON a) => BS.ByteString -> Either BS.ByteString a
tryDecode i =
    let latin1 = convert "LATIN1" "UTF-8" li
        li = BSL.fromStrict i
        o = case decodeUtf8' i of
                Left _  -> decode latin1
                Right _ -> decode li
    in case o of
           Just x  -> Right x
           Nothing -> Left i

-- | This creates a logstash network listener, given a TCP port.
-- It will try to decode the Bytestring as UTF-8, and, if it fails, as
-- Latin1.
logstashListener :: Int -- ^ Port number
                 -> Sink (Either BS.ByteString LogstashMessage) IO ()
                 -> IO ()
logstashListener port sink = runTCPServer (serverSettings port "*") (\app -> appSource app $= CB.lines $= CL.map tryDecode $$ sink)
