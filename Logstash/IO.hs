{-| This module needs a lot of work. It will contain all the functions that
are needed to send some "LogstashMessage" to a Logstash server.
-}
module Logstash.IO where

import Logstash.Message
import Network
import qualified Data.ByteString.Lazy as BSL
import System.IO
import Data.Aeson

{-| This very simple function lets you send a single message to a Logstash
server, using the tcp input, configured in the following way:

> input {
>   tcp {
>     debug        => "true"
>     port         => "12345"
>     data_timeout => -1
>     format       => "json_event"
>     type         => "somemessages"
>   }
> }
-}
sendSingleMessage :: HostName -> PortID -> LogstashMessage -> IO ()
sendSingleMessage h p m = do
    handle <- connectTo h p
    BSL.hPutStr handle (encode m)
    hClose handle

