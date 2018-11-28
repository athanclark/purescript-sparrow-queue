module Sparrow.Client.Queue where

import Sparrow.Types (Client, staticClient)
import Queue.Types (readOnly, writeOnly, allowReading, allowWriting)
import Queue.One.Aff as OneIO
import Queue.One as One
import Queue (READ, WRITE)

import Prelude
import Data.Maybe (Maybe (..))
import Data.Either (Either (..))
import Data.Argonaut.JSONVoid (JSONVoid)
import Effect (Effect)
import Effect.Aff (Aff, runAff_)
import Effect.Console (warn)
import Effect.Ref as Ref
import Effect.Class (liftEffect)


type SparrowStaticClientQueues initIn initOut =
  OneIO.IOQueues initIn (Maybe initOut)


type SparrowClientQueues initIn initOut deltaIn deltaOut =
  { init :: SparrowStaticClientQueues initIn initOut
  , deltaIn :: One.Queue (write :: WRITE) deltaIn
  , deltaOut :: One.Queue (read :: READ) deltaOut
  , onReject :: One.Queue (read :: READ) Unit
  , unsubscribe :: One.Queue (write :: WRITE) Unit
  }


newSparrowStaticClientQueues :: forall initIn initOut
                              . Effect (SparrowStaticClientQueues initIn initOut)
newSparrowStaticClientQueues = OneIO.new


newSparrowClientQueues :: forall initIn initOut deltaIn deltaOut
                        . Effect (SparrowClientQueues initIn initOut deltaIn deltaOut)
newSparrowClientQueues = do
  init <- newSparrowStaticClientQueues
  deltaIn <- writeOnly <$> One.new
  deltaOut <- readOnly <$> One.new
  unsubscribe <- writeOnly <$> One.new
  onReject <- readOnly <$> One.new
  pure {init, deltaIn, deltaOut, onReject, unsubscribe}



sparrowStaticClientQueues :: forall initIn initOut
                           . SparrowStaticClientQueues initIn initOut
                          -> Client initIn initOut JSONVoid JSONVoid
sparrowStaticClientQueues (OneIO.IOQueues {input: initInQueue, output: initOutQueue}) =
  staticClient \invoke ->
    One.on initInQueue \initIn ->
      invoke initIn (One.put initOutQueue)


sparrowClientQueues :: forall initIn initOut deltaIn deltaOut
                     . SparrowClientQueues initIn initOut deltaIn deltaOut
                    -> Client initIn initOut deltaIn deltaOut
sparrowClientQueues
  { init: OneIO.IOQueues {input: initInQueue, output: initOutQueue}
  , deltaIn: deltaInQueue
  , deltaOut: deltaOutQueue
  , onReject: onRejectQueue
  , unsubscribe: unsubscribeQueue
  } = \register ->
  One.on initInQueue \initIn ->
    register
      { initIn
      , onReject: do
        One.del (allowReading deltaInQueue)
        One.del (allowReading unsubscribeQueue)
        One.put (allowWriting onRejectQueue) unit
      , receive: \_ deltaOut -> One.put (allowWriting deltaOutQueue) deltaOut
      }
      (\mReturn -> do
          case mReturn of
            Nothing -> One.put initOutQueue Nothing
            Just {initOut,sendCurrent,unsubscribe} -> do
              One.put initOutQueue (Just initOut)
              One.on (allowReading deltaInQueue) sendCurrent
              One.on (allowReading unsubscribeQueue) \_ -> unsubscribe
          pure Nothing
      )


-- | Simplified version of calling queues, which doesn't care about unsubscribing
callSparrowClientQueues :: forall initIn initOut deltaIn deltaOut
                         . SparrowClientQueues initIn initOut deltaIn deltaOut
                        -> (deltaOut -> Effect Unit)
                        -> initIn
                        -> Aff
                              ( Maybe
                                { initOut     :: initOut
                                , deltaIn     :: deltaIn -> Effect Unit
                                , unsubscribe :: Effect Unit
                                }
                              )
callSparrowClientQueues {init,deltaIn,deltaOut,onReject,unsubscribe} onDeltaOut initIn = do
  mInitOut <- OneIO.callAsync init initIn
  case mInitOut of
    Nothing -> pure Nothing
    Just initOut -> do
      liftEffect do
        One.on deltaOut onDeltaOut
        One.once onReject \_ -> One.del deltaOut
      pure $ Just
        { initOut
        , deltaIn: One.put deltaIn
        , unsubscribe: do
          One.del deltaOut
          One.put unsubscribe unit
        }


-- | Be open as long as possible, while disallowing multiple init invocations while
-- | a subscription is open. The only way to dismantle is through killing the sub, via
-- | the result action
mountSparrowClientQueuesSingleton :: forall initIn initOut deltaIn deltaOut
                                   . SparrowClientQueues initIn initOut deltaIn deltaOut
                                  -> One.Queue (write :: WRITE) deltaIn
                                  -> One.Queue (write :: WRITE) initIn
                                  -> (deltaOut -> Effect Unit)
                                  -> (Maybe initOut -> Effect Unit)
                                  -> Effect (Effect Unit) -- completely destroy singleton - idempotent
mountSparrowClientQueuesSingleton queues deltaInQueue initInQueue onDeltaOut onInitOut = do
  subRef <- Ref.new Nothing
  One.on (allowReading initInQueue) \initIn -> do
    mUnsub <- Ref.read subRef
    case mUnsub of
      Just _ -> pure unit
      Nothing -> do -- continue if no sub exists
        let resolve eX = case eX of
              Left e -> warn ("callSparrowClientQueues from mount failed: " <> show e)
              Right _ -> pure unit
        runAff_ resolve do
          mResult <- callSparrowClientQueues queues onDeltaOut initIn
          case mResult of
            Nothing -> liftEffect do
              Ref.write Nothing subRef
              onInitOut Nothing
            Just {initOut,deltaIn: onDeltaIn,unsubscribe} -> do
              liftEffect do
                Ref.write (Just unsubscribe) subRef
                onInitOut (Just initOut) -- fix race conditions
                One.on (allowReading deltaInQueue) onDeltaIn
  pure do
    mUnsub <- Ref.read subRef
    case mUnsub of
      Nothing -> pure unit
      Just unsubscribe -> do
        One.del (allowReading deltaInQueue)
        unsubscribe
        Ref.write Nothing subRef
