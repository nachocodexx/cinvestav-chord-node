package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import fs2.{Stream, hash}
import io.circe.DecodingFailure
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.payloads.Payloads.AddKey
import mx.cinvestav.utils.Command
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.{Chord, ChordNodeInfo, CommandId}

import java.math.BigInteger
//import mx.cinvestav.utils.


class AddKeyHandler(command:Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler[IO,AddKey]{

  def belongsToMeFn(payload:AddKey):IO[Unit] = for {
    _ <- ctx.logger.debug("BELONGS TO ME")
    _ <- ctx.state.update(s=>s.copy(data =  s.data + (payload.key -> payload.value) ))
  } yield ()
  def belongsToOtherFn(payload:AddKey,chordNodeInfo: ChordNodeInfo):IO[Unit] = for {
    _            <- ctx.logger.debug(s"REDIRECT_ADD_KEY_COMMAND ${chordNodeInfo.chordId}")
    routingKey   = s"${ctx.config.poolId}.${chordNodeInfo.chordNodeId}.default"
    cmd          = command.asJson.noSpaces
    exchangeName = ctx.config.poolId
    publisher    <- ctx.utils.fromNodeIdToPublisher(
      nodeId = chordNodeInfo.chordNodeId,
      exchangeName = exchangeName,
      routingKey = routingKey
    )
    _ <- publisher.publish(cmd)
  } yield ()

  def simpleLookup(payload:AddKey):IO[Unit] = for {
    _ <- ctx.logger.debug("SIMPLE_LOOKUP_POLICY")
  } yield ()


  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: AddKey): IO[Unit] = for {
    _       <- ctx.logger.debug(CommandId.ADD_KEY+s" ${payload.key} ${payload.value}")
    currentState <- ctx.state.get
    keyHashBytes   = Stream.emits(payload.key.getBytes).through(hash.sha1).toVector.toArray
    failSteps      = currentState.fingerTable.map(chordData => chordData._2.step)
    defaultSteps   = currentState.totalOfNodes*ctx.config.chordStep
    totalOfDataItems = failSteps.sum+defaultSteps
    m              = new BigInteger(ctx.config.totalOfNodes.toString)
    keyHashInteger = new BigInteger(keyHashBytes)
    dataId    = keyHashInteger.mod(new BigInteger(totalOfDataItems.toString)).intValue()

    belongsToMe = Chord.checkIfBelongsToChordNode(dataId,currentState.chordInfo)
    belongToSucc = Chord.checkIfBelongsToChordNode(dataId,currentState.successor)
    belongToPred = Chord.checkIfBelongsToChordNode(dataId,currentState.predecessor)
        _ <- if(belongsToMe) belongsToMeFn(payload)
        else if (belongToSucc) belongsToOtherFn(payload,currentState.successor)
        else if (belongToPred) belongsToOtherFn(payload,currentState.predecessor)
        else ctx.config.lookupPolicy match {
          case Chord.policies.SIMPLE => simpleLookup(payload)
          case Chord.policies.DEFAULT => ctx.logger.error("NOT_IMPLEMENTED_YET")
          case Chord.policies.SCALABLE => ctx.logger.error("NOT_IMPLEMENTED_YET")
//          case a=> ctx.logger.error(a)
        }
    _       <- ctx.logger.info(s"DATA ID: $dataId")
    _       <- ctx.logger.info(s"TOTAL OF ITEMS: $totalOfDataItems")
    _      <- ctx.logger.info(currentState.chordInfo.toString)
//    _       <- ctx.logger.info(keyHashInteger.toString)
//    _       <- ctx.logger.info(.toString)
//    _ <- ctx.state.update(s=>s.copy(data = s.data +  (payload.key -> payload.value)))
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[AddKey])
}

object AddKeyHandler{
  def apply(command: Command[Json])(implicit ctx:NodeContext[IO]) =
    new AddKeyHandler(command = command).handle()
}
