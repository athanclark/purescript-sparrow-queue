module Sparrow.Client.Queue where

import Sparrow.Types (Client)
import Sparrow.Client.Types (SparrowClientT)
import Queue.One.Aff as OneIO
import Queue.One as One
import Queue (READ, WRITE)

import Prelude
import Data.Maybe (Maybe (..))
import Data.Functor.Singleton (class SingletonFunctor, liftBaseWith_)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Ref (REF)
import Control.Monad.Eff.Class (class MonadEff, liftEff)
import Control.Monad.Trans.Control (class MonadBaseControl)


type SparrowClientQueues eff initIn initOut deltaIn deltaOut =
  { init :: OneIO.IOQueues eff initIn (Maybe initOut)
  , deltaIn :: One.Queue (write :: WRITE) eff deltaIn
  , deltaOut :: One.Queue (read :: READ) eff deltaOut
  , onReject :: One.Queue (read :: READ) eff Unit
  , unsubscribe :: One.Queue (write :: WRITE) eff Unit
  }


newSparrowClientQueues :: forall eff initIn initOut deltaIn deltaOut
                        . Eff (ref :: REF | eff) (SparrowClientQueues (ref :: REF | eff) initIn initOut deltaIn deltaOut)
newSparrowClientQueues = do
  init <- OneIO.newIOQueues
  deltaIn <- One.writeOnly <$> One.newQueue
  deltaOut <- One.readOnly <$> One.newQueue
  unsubscribe <- One.writeOnly <$> One.newQueue
  onReject <- One.readOnly <$> One.newQueue
  pure {init, deltaIn, deltaOut, onReject, unsubscribe}


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
        One.delQueue $ One.allowReading deltaInQueue
        One.delQueue initInQueue
        One.delQueue $ One.allowReading unsubscribeQueue
        One.putQueue (One.allowWriting onRejectQueue) unit
      , receive: \_ deltaOut -> liftEff $ One.putQueue (One.allowWriting deltaOutQueue) deltaOut
      }
      (\mReturn -> do
          case mReturn of
            Nothing -> liftEff $ do
              One.putQueue initOutQueue Nothing
            Just {initOut,sendCurrent,unsubscribe} -> liftEff $ do
              One.putQueue initOutQueue (Just initOut)
              One.onQueue (One.allowReading deltaInQueue) (runM <<< sendCurrent)
              One.onQueue (One.allowReading unsubscribeQueue) \_ -> runM unsubscribe
          pure Nothing
      )
