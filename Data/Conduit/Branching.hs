-- | Branching conduits ...
module Data.Conduit.Branching (mkBranchingConduit) where

import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Control.Concurrent (MVar, putMVar, takeMVar, newEmptyMVar)
import qualified Data.IntMap as IntMap
import Data.Maybe (mapMaybe)

mkBranchingConduit :: (MonadResource m)
                    => Int -- ^ Number of branches
                    -> (a -> [Int]) -- ^ Branching function, where 0 is the first branch
                    -> IO (Sink a m (), [Source m a]) -- ^ Returns a sink and N sources
mkBranchingConduit nbbranches brfunction = do
    mvars <- replicateM nbbranches newEmptyMVar
    return (mvarSink brfunction mvars, map mvarSource mvars)

mvarSink :: (MonadResource m) => (a -> [Int]) -> [MVar (Maybe a)] -> Sink a m ()
mvarSink brfunc mvs =
    let mvarmap = IntMap.fromList (zip [0..] mvs)
        doBranch input =
            let channels = brfunc input
                mvars    = mapMaybe (\x -> IntMap.lookup x mvarmap) channels
            in  mapM_ (\mv -> liftIO $ putMVar mv (Just input)) mvars
    in  bracketP (return ()) (const $ mapM_ (\mv -> putMVar mv Nothing) mvs) (const $ CL.mapM_ doBranch)

mvarSource :: (MonadResource m) => MVar (Maybe a) -> Source m a
mvarSource mv = do
    v <- liftIO $ takeMVar mv
    case v of
        Just x -> yield x >> mvarSource mv
        Nothing -> return ()
