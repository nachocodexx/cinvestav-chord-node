package mx.cinvestav

import cats.data.EitherT
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.model.AmqpEnvelope
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{LongVal, StringVal}
import mx.cinvestav.Declarations.{NoReplyTo, NodeContextV5, NodeError, NodeStateV5}
import mx.cinvestav.commons.types.LookupResult
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import io.circe._
import io.circe.generic.auto._
import mx.cinvestav.commons.liftFF
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.commons.payloads.{v2 => payloads}

import java.math.BigInteger
import java.util.UUID
//NgXAYtSd7JK6
//$1$9IMYrTig$iUET1Vy6lDq2t7m1K4amL1

object CommandHandlers {

  def addKey()(implicit ctx:NodeContextV5,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCB(payload:payloads.AddKey):IO[Unit] = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      val messageId                = envelope.properties.messageId.getOrElse(UUID.randomUUID().toString)
      val visitedNodes             = envelope.properties.headers
        .getOrElse("visitedNodes",StringVal(""))
        .toValueWriterCompatibleJava
        .asInstanceOf[String]
        .split(',')
        .toList
      implicit val rabbitMQContext = ctx.rabbitContext
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
//
      val app = for {
//      TIMESTAMP
        timestamp     <- liftFF[Long,E](IO.realTime.map(_.toMillis))
//      INIT_TIME
        initialTime  = envelope.properties.headers
          .getOrElse("timestamp",LongVal(timestamp))
          .toValueWriterCompatibleJava.asInstanceOf[Long]
//      CURRENT_STATE
        currentState  <- maybeCurrentState
        chordNodeHash = currentState.chordNode.hash
        fingerTable   = currentState.fingerTable
//      PAYLOAD
        key          = payload.key
        value        = payload.value
        mSlots       = new BigInteger(ctx.config.chordSlots.toString)
        hashedKey    <- liftFF[BigInteger,NodeError](Helpers.strToHash(key,mSlots))
        n            = currentState.chordNode
//        lookupResult <- Helpers.localLookup(key)
        lookupResult = Helpers.findSuccessor(mSlots = mSlots.intValue(),n=n,key=hashedKey,fingerTable = fingerTable)
//        messageId    = ""
        _            <- L.info(s"ADD_KEY_LATENCY $messageId $key ${timestamp - payload.timestamp} ${timestamp - initialTime}")
        //      ___________________________________________________________________________
        _             <- L.debug(s"CHORD_HASH_ID $chordNodeHash")
        _             <- L.debug(s"HASH_KEY $hashedKey")
        _             <- L.debug(s"KEY_BELONGS_ME ${lookupResult.belongsToMe}")
        _             <- L.debug(s"KEY_BELONGS_v2 ${lookupResult.chordNode.nodeId}")
//        _             <- L.debug(s"KEY_BELONGS_OTHERS ${lookupResult.belongsToOthers}")
        _             <- L.debug(s"VALUE $value")
//        _             <- L.debug(s"VISITED_NODES ${visitedNodes.mkString(",")}")
        //      ______________________________________________________________________________
        _             <- if(lookupResult.belongsToMe)
                              liftFF[Unit,NodeError](Helpers.localAddKey(messageId,key,value,initialTime))
                        else liftFF[Unit,E](Helpers.addKey(
                                    messageId   = messageId,
                                    chordNode =  lookupResult.chordNode,
                                    key         = key,
                                    value       = value,
                                    initialTime = initialTime,
                                    visitedNodes  = visitedNodes
                        )
        )
      } yield (lookupResult)
      app.value.stopwatch.flatMap{ res=>
        res.result match {
          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>for {
            _ <- acker.ack(envelope.deliveryTag)
            key =payload.key
            duration = res.duration.toMillis
            _ <- ctx.logger.info(s"ADD_KEY $messageId $key $duration")
          } yield ()
        }
      }
    }
    processMessageV2[IO,payloads.AddKey,NodeContextV5](
      successCallback = successCB,
      errorCallback = e=>acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
    )
  }


  def lookup()(implicit ctx:NodeContextV5,envelope:AmqpEnvelope[String],acker:Acker): IO[Unit] = {
    def successCB(payload:payloads.Lookup):IO[Unit] = {
      type E                       = NodeError
      val unit                     = liftFF[Unit,NodeError](IO.unit)
      val maybeCurrentState        = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      val messageId                = envelope.properties.messageId.getOrElse(UUID.randomUUID().toString)
      val maybeReplyTo             = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val visitedNodes             = envelope.properties.headers
        .getOrElse("visitedNodes",StringVal(""))
        .toValueWriterCompatibleJava
        .asInstanceOf[String]
//        .filter(x=>x)
        .split(',')
        .toList
      implicit val logger          = ctx.logger
      val L                        = Logger.eitherTLogger[IO,E]
      val app = for {
//       TIMESTAMP
        timestamp     <- liftFF[Long,E](IO.realTime.map(_.toMillis))
        //       NODE_ID
        nodeId        = ctx.config.nodeId
        //       CURRENT_STATE
        currentState  <- maybeCurrentState
        //       INITIAL_TIME
        initialTime  = envelope.properties.headers
           .getOrElse("timestamp",LongVal(timestamp))
           .toValueWriterCompatibleJava.asInstanceOf[Long]
        //       REPLY_TO
        replyTo       <- maybeReplyTo
        //       NODE_HASH
        chordNodeHash = currentState.chordNode.hash
        //      NUM_CHORD_NODES
        chordNodesLen = ctx.config.chordNodes.length
        //       KEY
        key           = payload.key
        hashedKey     <- liftFF[BigInteger,E](Helpers.strToHash(key,new BigInteger(ctx.config.chordSlots.toString)))
        //
        //        lookupResult <- Helpers.localLookup(key)
        lookupResult      = Helpers.findSuccessor(ctx.config.chordSlots,currentState.chordNode,hashedKey,currentState.fingerTable)
        _             <- L.info(s"LOOKUP_LATENCY $messageId $key ${timestamp - payload.timestamp} ${timestamp-timestamp}")
        //      ___________________________________________________________________________
        _             <- L.debug(s"MESSAGE_ID $messageId")
        _             <- L.debug(s"CHORD_HASH_ID $chordNodeHash")
        _             <- L.debug(s"HASH_KEY $hashedKey")
        //        _             <- L.debug(s"KEY_BELONGS_ME ${lookupResult.belongsToMe}")
        _             <- L.debug(s"KEY_BELONGS_ME_v2 ${lookupResult.belongsToMe}")
        //        _             <- L.debug(s"KEY_BELONGS_OTHERS ${lookupResult.belongsToOthers}")
        _             <- L.debug(s"KEY_BELONGS ${lookupResult.chordNode.nodeId}")
        _             <- L.debug(s"VISITED_NODES ${visitedNodes.mkString(",")}")
        //      ______________________________________________________________________________
        _             <- if(visitedNodes.length == chordNodesLen)  liftFF[Unit,E](Helpers.keyNotFound(key=key,replyTo = replyTo,arrivalTime = initialTime,visitedNodes = visitedNodes))
                        else if(lookupResult.belongsToMe ) liftFF[Unit,E](Helpers.foundKey(key,replyTo,visitedNodes=visitedNodes,arrivalTime =initialTime ))
                        else liftFF[Unit,E](Helpers.lookup(lookupResult.chordNode,key,replyTo,messageId,visitedNodes=visitedNodes))
//                        else liftFF[Unit,E](Helpers.lookup(lookupResult.belongsToOthers,key,replyTo,messageId,visitedNodes))
      } yield ()
      app.value.stopwatch.flatMap{ res=>
        res.result match {
          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>for {
            _ <- acker.ack(envelope.deliveryTag)
            _ <- ctx.logger.info(s"LOOKUP $messageId ${payload.key} ${res.duration}")
          } yield ()
        }
      }
    }
    processMessageV2[IO,payloads.Lookup,NodeContextV5](
      successCallback = successCB,
      errorCallback = e=>acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
    )
  }

}
