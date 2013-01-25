module Data.Conduit.Logstash (logstashListener) where

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

tryDecode :: (FromJSON a) => BS.ByteString -> Maybe a
tryDecode i =
    let latin1 = convert "LATIN1" "UTF-8" li
        li = BSL.fromStrict i
    in  case decodeUtf8' i of
            Left _  -> decode latin1
            Right _ -> decode li

logstashListener :: Int -> Sink LogstashMessage (ResourceT IO) () -> IO ()
logstashListener port sink = runResourceT $ runTCPServer (serverSettings port HostAny) (\app -> appSource app $= CB.lines $= CL.mapMaybe tryDecode $$ sink)
