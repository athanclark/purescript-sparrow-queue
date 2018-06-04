module Sparrow.Client.Queue where

import Sparrow.Types (Client, JSONVoid, staticClient)
import Queue.Types (readOnly, writeOnly, allowReading, allowWriting)
import Queue.One.Aff as OneIO
import Queue.One as One
import Queue (READ, WRITE)

import Prelude
import Data.Maybe (Maybe (..))
import Data.Either (Either (..))
import Data.Functor.Singleton (class SingletonFunctor, liftBaseWith_)
import Control.Monad.Aff (Aff, runAff_)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Ref (REF, newRef, writeRef, readRef)
import Control.Monad.Eff.Class (class MonadEff, liftEff)
import Control.Monad.Trans.Control (class MonadBaseControl)


type SparrowStaticClientQueues eff initIn initOut =
  OneIO.IOQueues eff initIn (Maybe initOut)


type SparrowClientQueues eff initIn initOut deltaIn deltaOut =
  { init :: SparrowStaticClientQueues eff initIn initOut
  , deltaIn :: One.Queue (write :: WRITE) eff deltaIn
  , deltaOut :: One.Queue (read :: READ) eff deltaOut
  , onReject :: One.Queue (read :: READ) eff Unit
  , unsubscribe :: One.Queue (write :: WRITE) eff Unit
  }


newSparrowStaticClientQueues :: forall eff initIn initOut
                              . Eff (Effects eff) (SparrowStaticClientQueues (Effects eff) initIn initOut)
newSparrowStaticClientQueues = OneIO.newIOQueues


newSparrowClientQueues :: forall eff initIn initOut deltaIn deltaOut
                        . Eff (Effects eff) (SparrowClientQueues (Effects eff) initIn initOut deltaIn deltaOut)
newSparrowClientQueues = do
  init <- newSparrowStaticClientQueues
  deltaIn <- writeOnly <$> One.newQueue
  deltaOut <- readOnly <$> One.newQueue
  unsubscribe <- writeOnly <$> One.newQueue
  onReject <- readOnly <$> One.newQueue
  pure {init, deltaIn, deltaOut, onReject, unsubscribe}


type Effects eff =
  ( ref :: REF
  | eff)


sparrowStaticClientQueues :: forall eff m stM initIn initOut
                           . MonadEff (Effects eff) m
                          => MonadBaseControl (Eff (Effects eff)) m stM
                          => SingletonFunctor stM
                          => SparrowStaticClientQueues (Effects eff) initIn initOut
                          -> Client (Effects eff) m initIn initOut JSONVoid JSONVoid
sparrowStaticClientQueues (OneIO.IOQueues {input: initInQueue, output: initOutQueue}) =
  staticClient \invoke -> liftBaseWith_ \runM ->
    One.onQueue initInQueue \initIn ->
      runM (invoke initIn (liftEff <<< One.putQueue initOutQueue))


sparrowClientQueues :: forall eff m stM initIn initOut deltaIn deltaOut
                     . MonadEff (Effects eff) m
                    => MonadBaseControl (Eff (Effects eff)) m stM
                    => SingletonFunctor stM
                    => SparrowClientQueues (Effects eff) initIn initOut deltaIn deltaOut
                    -> Client (Effects eff) m initIn initOut deltaIn deltaOut
sparrowClientQueues
  { init: OneIO.IOQueues {input: initInQueue, output: initOutQueue}
  , deltaIn: deltaInQueue
  , deltaOut: deltaOutQueue
  , onReject: onRejectQueue
  , unsubscribe: unsubscribeQueue
  } = \register -> liftBaseWith_ \runM ->
  One.onQueue initInQueue \initIn -> runM $
    register
      { initIn
      , onReject: liftEff $ do
        One.delQueue (allowReading deltaInQueue)
        One.delQueue (allowReading unsubscribeQueue)
        One.putQueue (allowWriting onRejectQueue) unit
      , receive: \_ deltaOut -> liftEff $ One.putQueue (allowWriting deltaOutQueue) deltaOut
      }
      (\mReturn -> do
          case mReturn of
            Nothing -> liftEff $ do
              One.putQueue initOutQueue Nothing
            Just {initOut,sendCurrent,unsubscribe} -> liftEff $ do
              One.putQueue initOutQueue (Just initOut)
              One.onQueue (allowReading deltaInQueue) (runM <<< sendCurrent)
              One.onQueue (allowReading unsubscribeQueue) \_ -> runM unsubscribe
          pure Nothing
      )


-- | Simplified version of calling queues, which doesn't care about unsubscribing
callSparrowClientQueues :: forall eff initIn initOut deltaIn deltaOut
                         . SparrowClientQueues (Effects eff) initIn initOut deltaIn deltaOut
                        -> (deltaOut -> Eff (Effects eff) Unit)
                        -> initIn
                        -> Aff (Effects eff)
                              ( Maybe
                                { initOut     :: initOut
                                , deltaIn     :: deltaIn -> Eff (Effects eff) Unit
                                , unsubscribe :: Eff (Effects eff) Unit
                                }
                              )
callSparrowClientQueues {init,deltaIn,deltaOut,onReject,unsubscribe} onDeltaOut initIn = do
  mInitOut <- OneIO.callAsync init initIn
  case mInitOut of
    Nothing -> pure Nothing
    Just initOut -> do
      liftEff $ do
        One.onQueue deltaOut onDeltaOut
        One.onceQueue onReject \_ -> One.delQueue deltaOut
      pure $ Just
        { initOut
        , deltaIn: One.putQueue deltaIn
        , unsubscribe: do
          One.delQueue deltaOut
          One.putQueue unsubscribe unit
        }


-- | Be open as long as possible, while disallowing multiple init invocations while
-- | a subscription is open. The only way to dismantle is through killing the sub, via
-- | the result action
mountSparrowClientQueuesSingleton :: forall eff initIn initOut deltaIn deltaOut
                                   . SparrowClientQueues (Effects eff) initIn initOut deltaIn deltaOut
                                  -> One.Queue (write :: WRITE) (Effects eff) deltaIn
                                  -> One.Queue (write :: WRITE) (Effects eff) initIn
                                  -> (deltaOut -> Eff (Effects eff) Unit)
                                  -> (Maybe initOut -> Eff (Effects eff) Unit)
                                  -> Eff (Effects eff) (Eff (Effects eff) Unit) -- completely destroy singleton - idempotent
mountSparrowClientQueuesSingleton queues deltaInQueue initInQueue onDeltaOut onInitOut = do
  subRef <- newRef Nothing
  One.onQueue (allowReading initInQueue) \initIn -> do
    mUnsub <- readRef subRef
    case mUnsub of
      Just _ -> pure unit -- don't do nufun if there's already sub
      Nothing -> do -- continue if no sub exists
        let resolve eX = case eX of
              Left e -> pure unit -- FIXME error?
              Right mUnsub' -> writeRef subRef mUnsub' -- dafuq?
        runAff_ resolve $ do
          mResult <- callSparrowClientQueues queues onDeltaOut initIn
          case mResult of
            Nothing -> do
              liftEff (onInitOut Nothing)
              pure Nothing
            Just {initOut,deltaIn: onDeltaIn,unsubscribe} -> do
              liftEff $ do
                onInitOut (Just initOut) -- fix race conditions
                One.onQueue (allowReading deltaInQueue) onDeltaIn
                pure (Just unsubscribe)
  pure $ do
    mUnsub <- readRef subRef
    case mUnsub of
      Nothing -> pure unit
      Just unsubscribe -> do
        writeRef subRef Nothing
        One.delQueue (allowReading deltaInQueue)
        unsubscribe
