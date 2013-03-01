{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Conduit.Network.Retry where

import Prelude hiding (catch)
import Data.Conduit
import Data.Conduit.Network
import Network.Socket (Socket, close)
import Network.Socket.ByteString (sendAll)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Exception
import Data.ByteString (ByteString)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Monad ((>=>))
import Control.Monad.Trans.Class (lift)

{-| Tentative /safe/ "Sink" for a "Socket". It should try reopening the "Socket"
every time the call to 'sendAll' fails. This means that some bytes might be sent
multiple times, if the socket fails in the middle of the sendAll call. This is
targeted at protocols where only a full message makes sense.

This is used to send a full JSON object to Logstash.
-}
sinkSocketRetry :: MonadResource m => IO Socket -> Int -> IO () -> Consumer ByteString m ()
sinkSocketRetry mkSocket delay exeptionCallback =
    let
        safeMkSocket :: IO Socket
        safeMkSocket = catch mkSocket (\SomeException{} -> exeptionCallback >> threadDelay delay >> safeMkSocket)
        safeSend :: MVar Socket -> ByteString -> IO ()
        safeSend s o = do
            sock <- takeMVar s
            catch (sendAll sock o >> putMVar s sock) $ \SomeException{} -> do
                close sock
                safeMkSocket >>= putMVar s
                threadDelay delay
                safeSend s o
        push :: MonadResource m => MVar Socket -> Consumer ByteString m ()
        push s = await >>= maybe (return ()) (\bs -> lift (liftIO $ safeSend s bs) >> push s)
    in  bracketP (safeMkSocket >>= newMVar) (takeMVar >=> close) push

-- | A specialization of the previous Sink that opens a TCP connection.
tcpSinkRetry :: MonadResource m => ByteString -> Int -> Int -> IO () -> Consumer ByteString m ()
tcpSinkRetry host port = sinkSocketRetry (fmap fst (getSocket host port))

