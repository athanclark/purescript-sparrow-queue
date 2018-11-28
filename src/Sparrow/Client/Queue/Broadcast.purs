module Sparrow.Client.Queue.Broadcast where

import Sparrow.Types (Client)
import Queue.Types (writeOnly, readOnly, allowReading, allowWriting)
import Queue.One as One
import IxQueue as Ix
import Queue (READ, WRITE)

import Prelude
import Data.Maybe (Maybe (..))
import Effect (Effect)


type SparrowClientQueues initIn initOut deltaIn deltaOut =
  { initIn :: One.Queue (write :: WRITE) initIn
  , initOut :: Ix.IxQueue (read :: READ) (Maybe initOut)
  , deltaIn :: One.Queue (write :: WRITE) deltaIn
  , deltaOut :: Ix.IxQueue (read :: READ) deltaOut
  , onReject :: Ix.IxQueue (read :: READ) Unit
  , unsubscribe :: One.Queue (write :: WRITE) Unit
  }


newSparrowClientQueues :: forall initIn initOut deltaIn deltaOut
                        . Effect (SparrowClientQueues initIn initOut deltaIn deltaOut)
newSparrowClientQueues = do
  initIn <- writeOnly <$> One.new
  initOut <- readOnly <$> Ix.new
  deltaIn <- writeOnly <$> One.new
  deltaOut <- readOnly <$> Ix.new
  unsubscribe <- writeOnly <$> One.new
  onReject <- readOnly <$> Ix.new
  pure {initIn, initOut, deltaIn, deltaOut, onReject, unsubscribe}


sparrowClientQueues :: forall initIn initOut deltaIn deltaOut
                     . SparrowClientQueues initIn initOut deltaIn deltaOut
                    -> Client initIn initOut deltaIn deltaOut
sparrowClientQueues
  { initIn: initInQueue
  , initOut: initOutQueue
  , deltaIn: deltaInQueue
  , deltaOut: deltaOutQueue
  , onReject: onRejectQueue
  , unsubscribe: unsubscribeQueue
  } = \register ->
  One.on (allowReading initInQueue) \initIn ->
    register
      { initIn
      , onReject: do
        One.del (allowReading deltaInQueue)
        One.del (allowReading unsubscribeQueue)
        Ix.broadcast (allowWriting onRejectQueue) unit
      , receive: \_ deltaOut -> Ix.broadcast (allowWriting deltaOutQueue) deltaOut
      }
      (\mReturn -> do
          case mReturn of
            Nothing -> Ix.broadcast (allowWriting initOutQueue) Nothing
            Just {initOut,sendCurrent,unsubscribe} -> do
              Ix.broadcast (allowWriting initOutQueue) (Just initOut)
              One.on (allowReading deltaInQueue) sendCurrent
              One.on (allowReading unsubscribeQueue) \_ -> unsubscribe
          pure Nothing
      )
