{-| Branching conduits

WARNING: executables using this function must be compiled with -threaded
-}
module Data.Conduit.Branching (mkBranchingConduit, branchConduits) where

import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import qualified Data.IntMap as IntMap
import Data.Maybe (mapMaybe)
import Control.Concurrent.ParallelIO
import Data.Conduit.TMChan
import GHC.Conc (atomically)

mkSink :: (MonadResource m)
       => (a -> [Int])
       -> [TBMChan a]
       -> Sink a m ()
mkSink brfunc chans =
    let cmap       = IntMap.fromList $ zip [0..] chans -- creates an intmap with all output channels
        querymap x = IntMap.lookup x cmap              -- query the intmap to get a Maybe (TBMChan a)
        cleanup = mapM_ (liftIO . atomically . closeTBMChan) chans -- this is the cleanup function that closes all chans
        inject input =
            let outchans = mapMaybe querymap (brfunc input) -- compiles the list of output channels
            in  mapM_ (\c -> liftIO $ atomically $ writeTBMChan c input) outchans -- and write into them
    in  addCleanup (const cleanup) (CL.mapM_ inject)

mkBranchingConduit :: (MonadResource m)
                    => Int -- ^ Number of branches
                    -> (a -> [Int]) -- ^ Branching function, where 0 is the first branch
                    -> IO (Sink a m (), [Source m a]) -- ^ Returns a sink and N sources
mkBranchingConduit nbbranches brfunction = do
    chans <- replicateM nbbranches (newTBMChanIO 16)
    return (mkSink brfunction chans, map sourceTBMChan chans)

branchConduits :: Source (ResourceT IO) a       -- ^ The source to branch from
               -> (a -> [Int])                  -- ^ The branching function (0 is the first sink)
               -> [Sink a (ResourceT IO) ()]    -- ^ The destination sinks
               -> IO ()                         -- ^ Results of the sinks
branchConduits src brfunc sinks = do
    (newsink, sources) <- mkBranchingConduit (length sinks) brfunc
    let srcconduit = src $$ newsink
        dstconduits = map (uncurry ($$)) (zip sources sinks)
        actions = map runResourceT (srcconduit : dstconduits)
    parallel_ actions
