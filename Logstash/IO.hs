module Logstash.IO where

import Logstash.Message
import Network
import qualified Data.ByteString.Lazy as BSL
import System.IO
import Data.Aeson

sendSingleMessage :: HostName -> PortID -> LogstashMessage -> IO ()
sendSingleMessage h p m = do
    handle <- connectTo h p
    BSL.hPutStr handle (encode m)
    hClose handle

