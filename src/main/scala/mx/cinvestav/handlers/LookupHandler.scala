package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.{Chord, ChordNodeInfo}
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.commons.payloads.{Lookup,KeyFound}
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.utils.Command
import fs2.Stream
import fs2.hash
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.payloads.Payloads

import java.math.BigInteger
class LookupHandler(command: Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler [IO,Lookup]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  def found(payload:Lookup,value:String): IO[Unit] = for {
//    _              <- resetNotFoundFlag(payload.key)
   _               <- ctx.logger.debug(Identifiers.KEY_FOUND+s" ${payload.id} ${payload.key} ${payload.experimentId}")
   timestamp       <- IO.realTime.map(_.toSeconds)
   publisher       <- ctx.utils.createPublisher(payload.exchangeName,payload.replyTo)
   keyFoundPayload =  KeyFound(id=payload.id,
     key=payload.key,
     value=value,
     timestamp= timestamp,
     chordNodeId= ctx.config.nodeId,
     experimentId=payload.experimentId
   )
   cmd             = CommandData[Json](Identifiers.KEY_FOUND,keyFoundPayload.asJson)
    _              <- publisher(cmd.asJson.noSpaces)
  } yield ()
  def continueLookup(payload:Lookup, idChordNode:ChordNodeInfo):IO[Unit] = for {
//    _  <- ctx.state.update(s=>s.copy(notFoundKeysCounter = s.notFoundKeysCounter.updated(payload.key,true)))
    _<- IO.unit
    cmd = CommandData[Json](command.commandId,payload.asJson)
    lookupParams = Chord.LookupParams(id=payload.id,experimentId = payload.experimentId,key= payload.key,idChordNode = idChordNode,cmd=cmd)
    _  <- ctx.config.lookupPolicy match {
      case Chord.policies.SIMPLE => Chord.simpleLookup(lookupParams)
      case Chord.policies.SCALABLE => Chord.scalableLookup(lookupParams)
      case Chord.policies.DEFAULT => Chord.defaultLookup(lookupParams)
      case _ => Chord.simpleLookup(lookupParams)
      //      case Chord.policies.DEFAULT =>
    }
  } yield ()

//  def resetNotFoundFlag(key:String): IO[Unit] = ctx.state.update(s=>s.copy(notFoundKeysCounter = s.notFoundKeysCounter.removed(key)))
  def notFound(payload:Lookup):IO[Unit] = for {
//    _          <- resetNotFoundFlag(payload.key)
    _          <- ctx.logger.debug(Identifiers.NOT_FOUND_KEY+s" ${payload.key}")
    timestamp  <- IO.realTime.map(_.toSeconds)
    cmdPayload = Payloads.KeyNotFound(payload.key,timestamp,ctx.config.nodeId)
    cmd        = CommandData[Json](Identifiers.NOT_FOUND_KEY,cmdPayload.asJson)
    publisher  <- ctx.utils.createPublisher(exchangeName =payload.exchangeName ,routingKey = payload.replyTo)
    _          <- publisher(cmd.asJson.noSpaces)
  } yield ()

  override def handleRight(payload: Lookup): IO[Unit] = for {
     _              <- ctx.logger.debug(Identifiers.LOOKUP+s" ${payload.id} ${payload.key} ${payload.experimentId}")
     currentState   <- ctx.state.get
     keyHashBytes   = Stream.emits(payload.key.getBytes).through(hash.sha1).toVector.toArray
     keyHashInteger = new BigInteger(keyHashBytes)
     dataId         = keyHashInteger.mod(new BigInteger(currentState.totalOfKeyIds.toString)).intValue()
     idChordNode    = ChordNodeInfo(-1,dataId,dataId)
     belongsToMe    = currentState.chordInfos.find(Chord.checkIfBelongsToChordNode(dataId,_))
     value          = currentState.data.get(payload.key)
     _ <- ctx.logger.info(idChordNode.toString)
     _  <- if(belongsToMe.isDefined && value.isDefined)  found(payload,value.get)
           else for {
              _<- IO.unit
              newPayloadLookup = payload.copy(lookupTrace = payload.lookupTrace:+ctx.config.nodeId)
              _<- if(payload.lookupTrace.contains(ctx.config.nodeId)) notFound(newPayloadLookup)
                   else continueLookup(newPayloadLookup,idChordNode)
          } yield ()
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Lookup])
}

object LookupHandler {
  def apply(command:Command[Json])(implicit ctx:NodeContext[IO]): IO[Unit] =
    new LookupHandler(command=command).handle()
}
