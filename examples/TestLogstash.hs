module Main where

import Logstash.Message
import Logstash.IO
import System.Environment
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Aeson
import Network

main :: IO ()
main = do
    (hostname:port:mtype:msource:tags) <- getArgs
    content <- T.getContents
    let msg = LogstashMessage (T.pack mtype) (T.pack msource) (map T.pack tags) (object []) content Nothing
    sendSingleMessage hostname (PortNumber (fromIntegral (read port))) msg

