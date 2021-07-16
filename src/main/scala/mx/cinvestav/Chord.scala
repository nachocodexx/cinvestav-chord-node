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
  case class LookupParams(id:String,experimentId:Int,key:String, idChordNode:ChordNodeInfo,cmd:CommandData[Json])
  case class RedirectParams(id:String,experimentId:Int,key:String,chordNodeInfo: ChordNodeInfo,cmd:CommandData[Json])

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

//  def redirect(key:String,chordNodeInfo: ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
 def redirect(params: RedirectParams)(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    _            <- ctx.logger.debug(s"REDIRECT_COMMAND ${params.id} ${params.chordNodeInfo.chordId} ${params.experimentId}")
    routingKey   = s"${ctx.config.poolId}.chord.${params.chordNodeInfo.chordNodeId}"
    exchangeName = ctx.config.poolId
    publisher    <- ctx.utils.fromNodeIdToPublisher(
      nodeId       = params.chordNodeInfo.chordNodeId,
      exchangeName = exchangeName,
      routingKey   = routingKey
    )
    _            <- publisher.publish(params.cmd.asJson.noSpaces)
  } yield ()
//  def simpleLookup(key:String, idChordNode:ChordNodeInfo,cmd:CommandData[Json])(implicit ctx:NodeContext[IO]):IO[Unit] = for {
    def simpleLookup(params: LookupParams)(implicit ctx:NodeContext[IO]):IO[Unit] = for {
//    _ <- ctx.logger.debug(s"SIMPLE_LOOKUP_POLICY ${params.id} ${params.idChordNode} ${params.experimentId}")
    currentState <- ctx.state.get
    successorsDistances = Chord.getRelativeDistance(params.idChordNode,currentState.successors).foldLeft(0)((total,node)=>total+node.distance)
    predecessorsDistance = Chord.getRelativeDistance(params.idChordNode,currentState.predecessors).foldLeft(0)((total,node)=>total+node.distance)
    redirectParams = (node:ChordNodeInfo)=>RedirectParams(id= params.id,experimentId =params.experimentId,key=params.key,chordNodeInfo =node,cmd=params.cmd)
//    cmd          = CommandData[Json](command.commandId,command.payload)
    _ <- if(successorsDistances < predecessorsDistance) redirect(redirectParams(currentState.successors.head))
//      redirect(params.key,currentState.successors.head,params.cmd)
    else redirect(redirectParams(currentState.predecessors.head))
//      redirect(params.key,currentState.predecessors.head,params.cmd)
  } yield ()
  def defaultLookup(params: LookupParams)(implicit ctx:NodeContext[IO]):IO[Unit] = for {
//    _ <- ctx.logger.debug("DEFAULT_LOOKUP_POLICY")
    currentState    <- ctx.state.get
    chordNodes      = Chord.getRelativeDistance(params.idChordNode,currentState.successors++currentState.predecessors)
    closerChordNode = chordNodes.min
    redirectParams  = RedirectParams(id= params.id,experimentId = params.experimentId,key = params.key,chordNodeInfo = closerChordNode.chordNodeInfo,cmd=params.cmd)
//    _ <- redirect(params.key,closerChordNode.chordNodeInfo,params.cmd)
    _ <- redirect(params = redirectParams)
  } yield ()
  def scalableLookup(params: LookupParams)(implicit ctx:NodeContext[IO]):IO[Unit] = for {
//    _ <- ctx.logger.debug("SCALABLE_LOOKUP_POLICY")
    currentState    <- ctx.state.get
    chordNodes      = Chord.getRelativeDistance(params.idChordNode,currentState.chordsData
//      currentState.successors++currentState.predecessors
    )
    closerChordNode = chordNodes.min
    redirectParams = RedirectParams(id =params.id,experimentId = params.experimentId , key = params.key, chordNodeInfo = closerChordNode.chordNodeInfo,cmd = params.cmd)
//    _ <- redirect(key,closerChordNode.chordNodeInfo,params.cmd)
    _ <- redirect(params = redirectParams)
  } yield ( )
}
