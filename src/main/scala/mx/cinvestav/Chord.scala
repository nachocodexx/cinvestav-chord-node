package mx.cinvestav

import cats.effect.IO
import cats.implicits._
import cats.kernel.Order
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.payloads.Payloads.AddKey
object Chord {
  object policies {
    final val SIMPLE   = "simple"
    final val SCALABLE = "scalable"
    final val DEFAULT  = "default"
  }

  object status{
    sealed trait Status
    case object Up extends Status
    case object Lock extends Status
  }
  case class ChordNodeWithDistance(chordNodeInfo: ChordNodeInfo,distance:Int)
  implicit val chordNodeWithDistanceOrdered: Order[ChordNodeWithDistance] = Order.from[ChordNodeWithDistance]{ (x, y)=>
    if(x.distance==y.distance) 0
    else if (x.distance<y.distance) -1
    else 1
  }


  def seqChordInfo(keyIdsPerNode:Int, to:Int)(total:Int,incrementFn:Int=>Int): List[ChordNodeInfo] =
    (1 until to+1)
    .map(incrementFn)
    .map(_+total)
    .map(_%total)
    .map(buildChordInfoById(_,step = keyIdsPerNode))
    .toList

  def calculateDistance(cX:ChordNodeInfo, cY:ChordNodeInfo): Int =
    (cX.start-cY.start).abs + (cX.end-cY.end).abs

  def getRelativeDistance(idChordNode: ChordNodeInfo,xs:List[ChordNodeInfo]): List[ChordNodeWithDistance] = xs.foldl(List.empty[ChordNodeWithDistance]){ (xs, chordNode)=>
    val distance = Chord.calculateDistance(idChordNode,chordNode)
    xs :+ ChordNodeWithDistance(chordNode,distance)
  }

  //  case object
  def buildChordInfoById(chordId:Int, step:Int = 10):ChordNodeInfo = {
    val start = chordId*step
    val end  = (start +  step)-1
    ChordNodeInfo(chordId = chordId,start =  start, end =  end)
  }
  def checkIfBelongs(dataId:Int, chordId:Int, step:Int): Boolean = {
    val info = buildChordInfoById(chordId = chordId,step=step)
    ((info.start <= dataId) && (info.end >= dataId))
  }
  def checkIfBelongsToChordNode(dataId:Int,chordNode:ChordNodeInfo): Boolean = {
    ((chordNode.start <= dataId) && (chordNode.end >= dataId))
  }
  def chordIdWithPrefix(chordId:Int):String =s"ch-$chordId"

  def redirect(key:String,chordNodeInfo: ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _            <- ctx.logger.debug(s"REDIRECT_ADD_KEY ${chordNodeInfo.chordId} $key")
    routingKey   = s"${ctx.config.poolId}.chord.${chordNodeInfo.chordNodeId}"
    exchangeName = ctx.config.poolId
    publisher    <- ctx.utils.fromNodeIdToPublisher(
      nodeId = chordNodeInfo.chordNodeId,
      exchangeName = exchangeName,
      routingKey = routingKey
    )
    _ <- publisher.publish(cmd.asJson.noSpaces)
  } yield ()
  def simpleLookup(key:String, idChordNode:ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _ <- ctx.logger.debug(s"SIMPLE_LOOKUP_POLICY ${idChordNode}")
    currentState <- ctx.state.get
    successorsDistances = Chord.getRelativeDistance(idChordNode,currentState.successors).foldLeft(0)((total,node)=>total+node.distance)
    predecessorsDistance = Chord.getRelativeDistance(idChordNode,currentState.predecessors).foldLeft(0)((total,node)=>total+node.distance)
//    cmd          = CommandData[Json](command.commandId,command.payload)
    _ <- if(successorsDistances < predecessorsDistance) redirect(key,currentState.successors.head,cmd)
    else redirect(key,currentState.predecessors.head,cmd)
  } yield ()
  def defaultLookup(key:String, idChordNode:ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _ <- ctx.logger.debug("DEFAULT_LOOKUP_POLICY")
    currentState    <- ctx.state.get
    chordNodes      = Chord.getRelativeDistance(idChordNode,currentState.successors++currentState.predecessors)
    closerChordNode = chordNodes.min
    _ <- redirect(key,closerChordNode.chordNodeInfo,cmd)
  } yield ()
  def scalableLookup(key:String, idChordNode:ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _ <- ctx.logger.debug("SCALABLE_LOOKUP_POLICY")
    currentState    <- ctx.state.get
    chordNodes      = Chord.getRelativeDistance(idChordNode,currentState.fingerTable
//      currentState.successors++currentState.predecessors
    )
    closerChordNode = chordNodes.min
    _ <- redirect(key,closerChordNode.chordNodeInfo,cmd)
  } yield ( )
}
