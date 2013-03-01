module Data.Conduit.Misc where

import Data.Conduit
import Control.Monad

-- | Converts a stream of lists into a stream of single elements.
concat :: (Monad m) => Conduit [a] m a
concat = awaitForever (mapM_ yield)

-- | Converts a stream of [a] into a stream of (Flush a). This is done by
-- sending a Flush when the input is the empty list, or that we reached
-- a certain threshold
concatFlush :: (Monad m) => Integer -> Conduit [a] m (Flush a)
concatFlush mx = concatFlush' 0
    where
        concatFlush' x = await >>= maybe (return ()) (\input -> if null input
                                                                    then yield Flush >> concatFlush' 0
                                                                    else foldM sendf x input >>= concatFlush'
                                                  )
        sendf curx ev = do
            yield (Chunk ev)
            if curx >= mx
                then yield Flush >> return 0
                else return (curx+1)

-- | Regroup a stream of (Flush a) into a stream of lists, using "Flush" as
-- the separator
groupFlush :: (Monad m) => Conduit (Flush a) m [a]
groupFlush = grouper []
    where
        grouper lst = await >>= maybe (return ()) (handle lst)
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
