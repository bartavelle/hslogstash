-- | A firehose conduit, spawning a web server that will allow for the
-- observation of the messages.
module Data.Conduit.FireHose (fireHose) where

import Control.Monad.IO.Class

import Logstash.Message

import Data.Conduit
import Data.Conduit.Network.Firehose
import Network.Wai (pathInfo)

import qualified Data.HashSet as HS
import qualified Data.Text as T

import Data.Aeson
import Blaze.ByteString.Builder.ByteString
import Data.Monoid

{-| A web server will be launched on the specified port. Clients can
request URLs of the form /type1,type2,type3. They will be fed all
'LogstashMessage' matching one of the given types.

Here is a sample usage :

> -- run the fire hose on port 13400
> fh <- fireHose 13400 10
> logstashListener lport (printErrors =$ CL.mapM (liftIO . addLogstashTime) -- add the time
>                                     =$ fh
>                                     =$ CL.map (BSL.toStrict . encode) -- turn into a bytestring
>                                     =$ redisSink host port queue (Just logfunc)) -- store to redis

-}
fireHose :: MonadIO m => Int -- ^ Port
                      -> Int -- ^ Buffer size for the fire hose threads
                      -> IO (Conduit LogstashMessage m LogstashMessage)
fireHose port buffersize = firehoseConduit port buffersize getFilter serialize
    where
        serialize = (<> fromByteString "\n") . fromLazyByteString . encode
        getFilter r = case pathInfo r of
                          [p] -> let set = HS.fromList $ T.splitOn "," p
                                 in  flip HS.member set . logstashType
                          _ -> error "invalid url"

