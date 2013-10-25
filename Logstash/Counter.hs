{-| This module is not very well named, as it has almost nothing to do with
Logstash. It is used to define counters that will then be logged by collectd.

You should configure collectd to create a Unix socket :

> LoadPlugin unixsock
>
> <Plugin "unixsock">
>    SocketFile "/var/run/collectd-unixsock"
>    SocketGroup "collectdsocket"
>    SocketPerms "0660"
> </Plugin>

-}
module Logstash.Counter (Counter, newCounter, incrementCounterConduit, incrementCounter, readCounter, counter2collectd) where

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

-- | The opaque counter type. It is actually just a 'TVar' 'Integer'.
newtype Counter = Counter (TVar Integer)

-- | Gives you a new empty counter.
newCounter :: IO Counter
newCounter = fmap Counter $ newTVarIO 0

-- | This is a conduits-specific function that will increase a counter for
-- each piece of data that traverses this conduit. It will not alter the
-- data.
incrementCounterConduit :: (Monad m, MonadIO m) => Counter -> Conduit a m a
incrementCounterConduit c = CL.iterM (const $ liftIO $ incrementCounter c)

-- | Increments a counter.
incrementCounter :: Counter -> IO ()
incrementCounter (Counter c) = atomically $ modifyTVar c (+1)

-- | Retrieve the current value of a counter.
readCounter :: Counter -> IO Integer
readCounter (Counter c) = readTVarIO c

{-| This registers a counter to a Collectd server. This can be used in this way :

> counter2collectd nbmsg "/var/run/collectd-unixsock" nodename "logstash-shipper" "messages"
-}
counter2collectd :: Counter  -- ^ the counter to export
                 -> FilePath -- ^ path to the unix socket
                 -> String   -- ^ name of the node, usually the server's fully qualified domain name
                 -> String   -- ^ name of the plugin + instance
                 -> String   -- ^ name of the counter instance
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
                void $ T.hGetLine h
            threadDelay 10000000
            work

