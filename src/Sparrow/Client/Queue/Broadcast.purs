module Sparrow.Client.Queue.Broadcast where

import Sparrow.Types (Client)
import Sparrow.Client.Types (SparrowClientT)
import Queue.One as One
import IxQueue as Ix
import Queue (READ, WRITE)

import Prelude
import Data.Maybe (Maybe (..))
import Data.Functor.Singleton (class SingletonFunctor, liftBaseWith_)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Ref (REF)
import Control.Monad.Eff.Class (class MonadEff, liftEff)
import Control.Monad.Trans.Control (class MonadBaseControl)


type SparrowClientQueues eff initIn initOut deltaIn deltaOut =
  { initIn :: One.Queue (write :: WRITE) eff initIn
  , initOut :: Ix.IxQueue (read :: READ) eff (Maybe initOut)
  , deltaIn :: One.Queue (write :: WRITE) eff deltaIn
  , deltaOut :: Ix.IxQueue (read :: READ) eff deltaOut
  , onReject :: Ix.IxQueue (read :: READ) eff Unit
  , unsubscribe :: One.Queue (write :: WRITE) eff Unit
  }


newSparrowClientQueues :: forall eff initIn initOut deltaIn deltaOut
                        . Eff (ref :: REF | eff) (SparrowClientQueues (ref :: REF | eff) initIn initOut deltaIn deltaOut)
newSparrowClientQueues = do
  initIn <- One.writeOnly <$> One.newQueue
  initOut <- Ix.readOnly <$> Ix.newIxQueue
  deltaIn <- One.writeOnly <$> One.newQueue
  deltaOut <- Ix.readOnly <$> Ix.newIxQueue
  unsubscribe <- One.writeOnly <$> One.newQueue
  onReject <- Ix.readOnly <$> Ix.newIxQueue
  pure {initIn, initOut, deltaIn, deltaOut, onReject, unsubscribe}


type Effects eff =
  ( ref :: REF
  | eff)


sparrowClientQueues :: forall eff m stM initIn initOut deltaIn deltaOut
                     . MonadEff (Effects eff) m
                    => MonadBaseControl (Eff (Effects eff)) m stM
                    => SingletonFunctor stM
                    => SparrowClientQueues (Effects eff) initIn initOut deltaIn deltaOut
                    -> Client (Effects eff) m initIn initOut deltaIn deltaOut
sparrowClientQueues
  { initIn: initInQueue
  , initOut: initOutQueue
  , deltaIn: deltaInQueue
  , deltaOut: deltaOutQueue
  , onReject: onRejectQueue
  , unsubscribe: unsubscribeQueue
  } = \register -> liftBaseWith_ \runM ->
  One.onQueue (One.allowReading initInQueue) \initIn -> runM $
    register
      { initIn
      , onReject: liftEff $ do
        One.delQueue $ One.allowReading deltaInQueue
        One.delQueue $ One.allowReading unsubscribeQueue
        Ix.broadcastIxQueue (Ix.allowWriting onRejectQueue) unit
      , receive: \_ deltaOut -> liftEff $ Ix.broadcastIxQueue (Ix.allowWriting deltaOutQueue) deltaOut
      }
      (\mReturn -> do
          case mReturn of
            Nothing -> liftEff $ do
              Ix.broadcastIxQueue (Ix.allowWriting initOutQueue) Nothing
            Just {initOut,sendCurrent,unsubscribe} -> liftEff $ do
              Ix.broadcastIxQueue (Ix.allowWriting initOutQueue) (Just initOut)
              One.onQueue (One.allowReading deltaInQueue) (runM <<< sendCurrent)
              One.onQueue (One.allowReading unsubscribeQueue) \_ -> runM unsubscribe
          pure Nothing
      )
