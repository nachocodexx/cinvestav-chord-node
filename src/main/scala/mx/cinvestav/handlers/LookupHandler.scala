package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.{Chord, ChordNodeInfo, CommandId}
import mx.cinvestav.Main.{NodeContext, config}
import mx.cinvestav.payloads.Payloads.Lookup
import mx.cinvestav.utils.Command
import fs2.Stream
import fs2.hash
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.payloads.Payloads

import java.math.BigInteger
class LookupHandler(command: Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler [IO,Lookup]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  def found(payload:Lookup,value:String): IO[Unit] = for {
   _               <- ctx.logger.debug(s"KEY_FOUND ${payload.key}")
   timestamp       <- IO.realTime.map(_.toSeconds)
   publisher       <- ctx.utils.createPublisher(payload.exchangeName,payload.replyTo)
   keyFoundPayload =  Payloads.KeyFound(payload.key,value,timestamp,ctx.config.nodeId)
   cmd             = CommandData[Json](CommandId.KEY_FOUND,keyFoundPayload.asJson)
    _              <- publisher(cmd.asJson.noSpaces)
  } yield ()
  def continueLookup(payload:Lookup, idChordNode:ChordNodeInfo):IO[Unit] = for {
    _  <- ctx.state.update(s=>s.copy(notFoundKeysCounter = s.notFoundKeysCounter.updated(payload.key,true)))
    cmd = CommandData[Json](command.commandId,command.payload)
    _  <- ctx.config.lookupPolicy match {
      case Chord.policies.SIMPLE => Chord.simpleLookup(payload.key,idChordNode,cmd)
      case Chord.policies.SCALABLE => Chord.scalableLookup(payload.key,idChordNode,cmd)
      case Chord.policies.DEFAULT => Chord.defaultLookup(payload.key,idChordNode,cmd)
      case _ => Chord.simpleLookup(payload.key,idChordNode,cmd)
      //      case Chord.policies.DEFAULT =>
    }
  } yield ()

  def notFound(payload:Lookup):IO[Unit] = for {
    _         <- ctx.state.update(s=>s.copy(notFoundKeysCounter = s.notFoundKeysCounter.removed(payload.key)))
    _         <- ctx.logger.debug(s"KEY_NOT_FOUND ${payload.key}")
    timestamp <- IO.realTime.map(_.toSeconds)
    cmdPayload = Payloads.KeyNotFound(payload.key,timestamp,ctx.config.nodeId)
    cmd        = CommandData[Json](CommandId.NOT_FOUND_KEY,cmdPayload.asJson)
    publisher <- ctx.utils.createPublisher(exchangeName =payload.exchangeName ,routingKey = payload.replyTo)
    _ <- publisher(cmd.asJson.noSpaces)
  } yield ()

  override def handleRight(payload: Lookup): IO[Unit] = for {
     _              <- ctx.logger.debug(CommandId.LOOKUP+s" ${payload.key} ${payload.replyTo}")
     currentState   <- ctx.state.get
     keyHashBytes   = Stream.emits(payload.key.getBytes).through(hash.sha1).toVector.toArray
     keyHashInteger = new BigInteger(keyHashBytes)
     dataId         = keyHashInteger.mod(new BigInteger(currentState.totalOfKeyIds.toString)).intValue()
     idChordNode    = ChordNodeInfo(-1,dataId,dataId)
     belongsToMe    = currentState.chordInfos.find(Chord.checkIfBelongsToChordNode(dataId,_))
     value          = currentState.data.get(payload.key)
     _  <- if(belongsToMe.isDefined && value.isDefined)  found(payload,value.get)
     else
       for {
         _<-IO.unit
         maybeCounter = currentState.notFoundKeysCounter.get(payload.key)
         _ <- maybeCounter match {
           case Some(_) => notFound(payload)
           case None => continueLookup(payload,idChordNode)
         }
//        _ <-  notFound(payload,idChordNode)
       } yield()
//     else doestNotBelongsToMeFn(payload,idChordNode)
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Lookup])
}

object LookupHandler {
  def apply(command:Command[Json])(implicit ctx:NodeContext[IO]) =
    new LookupHandler(command=command).handle()
}
