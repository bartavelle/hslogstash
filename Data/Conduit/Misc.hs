module Data.Conduit.Misc where

import Data.Conduit
import Control.Monad

-- | Converts a stream of lists into a stream of single elements.
concat :: (Monad m) => Conduit [a] m a
concat = awaitForever (mapM_ yield)

-- | Converts a stream of [a] into a stream of (Flush a). This is done by
-- sending a Flush when the input is the empty list, or that we reached
-- a certain threshold
simpleConcatFlush :: (Monad m) => Int -> Conduit [a] m (Flush a)
simpleConcatFlush mx = concatFlush 0 sendf
    where
        sendf curx ev = do
            yield (Chunk ev)
            if curx >= (mx - 1)
                then yield Flush >> return 0
                else return (curx+1)

-- | This is a more general version of 'simpleConcatFlush', where you
-- provide your own fold.
concatFlush :: (Monad m) => b -> (b -> a -> ConduitM [a] (Flush a) m b) -> Conduit [a] m (Flush a)
concatFlush initial foldfunc = concatFlush' initial
    where
        concatFlush' x = await >>= maybe (return ()) (\input -> if null input
                                                                    then yield Flush >> concatFlush' initial
                                                                    else foldM foldfunc x input >>= concatFlush')

-- | A generalized version of 'simpleConcatFlush' where some value is
-- summed and the 'Flush' is sent when it reaches a threshold.
concatFlushSum :: (Num n, Ord n, Monad m)
               => (a -> n) -- ^ Convert your input value into an Integer, usually a size
               -> n -- ^ The threshold value
               -> Conduit [a] m (Flush a)
concatFlushSum tolength maxlength = concatFlush 0 foldfunc
    where
        foldfunc curlength element = do
            let nextlength = curlength + elementlength
                elementlength = tolength element
            if nextlength > maxlength
                then do
                    yield Flush
                    yield (Chunk element)
                    return elementlength
                else do
                    yield (Chunk element)
                    return nextlength

-- | Regroup a stream of (Flush a) into a stream of lists, using "Flush" as
-- the separator
groupFlush :: (Monad m) => Conduit (Flush a) m [a]
groupFlush = grouper []
    where
        grouper lst = await >>= maybe (unless (null lst) (yield (reverse lst))) (handle lst)
        handle lst Flush = do
            unless (null lst) (yield (reverse lst))
            grouper []
        handle lst (Chunk x) = grouper (x:lst)

-- | Analogous to maybe, but for chunks
mchunk :: b -> (a -> b) -> Flush a -> b
mchunk n _ Flush = n
mchunk _ f (Chunk x) = f x

-- | Like mapMaybe, but in a Flush. Will not touch the Flush values.
mapFlushMaybe :: (Monad m) => (a -> Maybe b) -> Conduit (Flush a) m (Flush b)
mapFlushMaybe f = awaitForever $ mchunk (yield Flush) (maybe (return ()) (yield . Chunk) . f)
