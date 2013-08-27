module Logstash.Counter where

import Control.Concurrent.STM
import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad.IO.Class
import Control.Concurrent
import System.IO
import Network.Socket
import Control.Monad
import Control.Exception
import Data.Time.Clock.POSIX
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Monoid

newtype Counter = Counter (TVar Integer)

newCounter :: IO Counter
newCounter = fmap Counter $ newTVarIO 0

incrementCounterConduit :: (Monad m, MonadIO m) => Counter -> Conduit a m a
incrementCounterConduit c = CL.iterM (const $ liftIO $ incrementCounter c)

incrementCounter :: Counter -> IO ()
incrementCounter (Counter c) = atomically $ modifyTVar c (+1)

readCounter :: Counter -> IO Integer
readCounter (Counter c) = readTVarIO c

counter2collectd :: Counter  -- the counter to export
                 -> FilePath -- path to the unix socket
                 -> String   -- name of the node
                 -> String   -- name of the plugin + instance
                 -> String   -- name of the counter instance
                 -> IO ()
counter2collectd c sockpath nodename plugin vinstance = void $ forkIO work
    where
        hdr = "PUTVAL " <> T.pack nodename <> "/" <> T.pack plugin <> "/derive-" <> T.pack vinstance <> " interval=10 "
        collectdConnect :: IO Handle
        collectdConnect = do
                soc <- socket AF_UNIX Stream 0
                connect soc (SockAddrUnix sockpath)
                socketToHandle soc ReadWriteMode
        work = do
            bracket collectdConnect hClose $ \h -> do
                v  <- readCounter c
                tt <- fmap (T.pack . show . (truncate :: (RealFrac a) => a -> Integer)) getPOSIXTime
                T.hPutStrLn h (hdr <> tt <> ":" <> T.pack (show v))
            threadDelay 10000000
            work

