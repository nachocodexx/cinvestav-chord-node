package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import fs2.{Stream, hash}
import io.circe.DecodingFailure
import mx.cinvestav.Main.NodeContext
//import mx.cinvestav.payloads.Payloads.AddKey
import mx.cinvestav.commons.payloads.AddKey
import mx.cinvestav.utils.Command
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.{Chord, ChordNodeInfo}
import mx.cinvestav.commons.commands.Identifiers

import java.math.BigInteger
//import mx.cinvestav.utils.


class AddKeyHandler(command:Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO,AddKey]{
  def doestNotBelongsToMeFn(payload:AddKey,idChordNode:ChordNodeInfo):IO[Unit] = for {
    _ <- IO.unit
    cmd = CommandData[Json](command.commandId,command.payload)
    _ <- ctx.config.lookupPolicy match {
      case Chord.policies.SIMPLE => Chord.simpleLookup(payload.key,idChordNode,cmd)
      case Chord.policies.SCALABLE => Chord.scalableLookup(payload.key,idChordNode,cmd)
      case Chord.policies.DEFAULT => Chord.defaultLookup(payload.key,idChordNode,cmd)
      case _ => Chord.simpleLookup(payload.key,idChordNode,cmd)
//      case Chord.policies.DEFAULT =>
    }
  } yield ()
  def belongsToMeFn(payload:AddKey):IO[Unit] = for {
    _ <- ctx.logger.debug(Identifiers.ADD_KEY+s" ${payload.key} ${payload.value}")
    _ <- ctx.state.update(s=>s.copy(data =  s.data + (payload.key -> payload.value) ))
  } yield ()



  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: AddKey): IO[Unit] = for {
    currentState    <- ctx.state.get
    keyHashBytes    = Stream.emits(payload.key.getBytes).through(hash.sha1).toVector.toArray
    keyHashInteger  = new BigInteger(keyHashBytes)
    dataId          = keyHashInteger.mod(new BigInteger(currentState.totalOfKeyIds.toString)).intValue()
    idChordNode     = ChordNodeInfo(-1,dataId,dataId)
    belongsToMe     = currentState.chordInfos.find(Chord.checkIfBelongsToChordNode(dataId,_))
    _  <- if(belongsToMe.isDefined) belongsToMeFn(payload)
    else doestNotBelongsToMeFn(payload,idChordNode)
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[AddKey])
}

object AddKeyHandler{
  def apply(command: Command[Json])(implicit ctx:NodeContext[IO]) =
    new AddKeyHandler(command = command).handle()
}
