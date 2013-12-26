-- | A firehose sink, letting client get through a port and read the sink
-- output.
module Data.Conduit.FireHose (fireHose) where

import Control.Monad.IO.Class

import Logstash.Message

import Data.Conduit
import Data.Conduit.Network.Firehose
import Network.Wai (Request,pathInfo)

import qualified Data.HashSet as HS
import qualified Data.Text as T

import Data.Aeson
import Blaze.ByteString.Builder.ByteString

-- | All clients connecting to the supplied port will start getting the
-- input of this conduit.
fireHose :: MonadIO m => Int -- ^ Port
                      -> Int -- ^Buffer size for the fire hose threads
                      -> IO (Conduit LogstashMessage m LogstashMessage)
fireHose port buffersize = firehoseConduit port buffersize getFilter serialize
    where
        serialize = fromLazyByteString . encode
        getFilter r = case pathInfo r of
                          [p] -> let set = HS.fromList $ T.splitOn "," p
                                 in  flip HS.member set . logstashType
                          _ -> error "invalid url"

