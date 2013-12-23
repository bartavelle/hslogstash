-- | A firehose sink, letting client get through a port and read the sink
-- output.
module Data.Conduit.FireHose (fireHose) where

import Control.Concurrent
import Control.Concurrent.STM
import Data.Conduit
import Data.Conduit.TMChan
import Data.Conduit.Network
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString as BS

-- | All clients connecting to the supplied port will start getting the
-- input of this conduit.
fireHose :: MonadIO m => Int -- ^ Port
                      -> IO (Sink BS.ByteString m ())
fireHose port = do
    chan <- atomically $ newTBMChan 16
    void $ forkIO $ runTCPServer (serverSettings port HostAny) (\ad -> sourceTBMChan chan $$ appSink ad)
    return (sinkTBMChan chan)

