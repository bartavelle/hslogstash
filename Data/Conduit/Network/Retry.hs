{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-| Network conduits that will retry sending messages forever -}
module Data.Conduit.Network.Retry where

import Prelude hiding (catch)
import Data.Conduit
import Network.Socket.ByteString (sendAll)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Exception
import Data.ByteString (ByteString)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Monad ((>=>))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Resource
import Network.Socket
import Network.BSD

{-| Tentative /safe/ "Sink" for a "Socket". It should try reopening the "Socket"
every time the call to 'sendAll' fails. This means that some bytes might be sent
multiple times, if the socket fails in the middle of the sendAll call. This is
targeted at protocols where only a full message makes sense.
-}
sinkSocketRetry :: MonadResource m => IO Socket -> Int -> IO () -> Consumer ByteString m ()
sinkSocketRetry mkSock delay exeptionCallback =
    let
        safeMkSocket :: IO Socket
        safeMkSocket = catch mkSock (\SomeException{} -> exeptionCallback >> threadDelay delay >> safeMkSocket)
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
tcpSinkRetry :: MonadResource m => String -> Int -> Int -> IO () -> Consumer ByteString m ()
tcpSinkRetry host port = sinkSocketRetry getSock
    where
        getSock = do
            proto <- getProtocolNumber "tcp"
            bracketOnError
                (socket AF_INET Stream proto)
                sClose
                (\sock -> do
                    he <- getHostByName host
                    Network.Socket.connect sock (SockAddrInet (fromIntegral port) (hostAddress he))
                    return sock
                )

