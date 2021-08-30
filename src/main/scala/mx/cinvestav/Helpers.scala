package mx.cinvestav

import cats.data.EitherT
import cats.implicits._
import cats.effect.IO
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpMessage, AmqpProperties, ExchangeName, RoutingKey}
import fs2.{Stream, hash}
import mx.cinvestav.Declarations.{ChordNode, FingerTableEntry, NodeContextV5, NodeError, NodeStateV5}
import mx.cinvestav.commons.types.{LookupResult, Metadata}
import mx.cinvestav.commons.liftFF
import org.typelevel.log4cats.Logger

import java.math.BigInteger
import mx.cinvestav.commons.nodes.Node
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, fromNodeToPublisher}
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.payloads.{v2 => payloads}
import mx.cinvestav.utils.v2.encoders._

object Helpers {


  def keyNotFound(key:String,replyTo:String,arrivalTime:Long=0L,visitedNodes:List[String]=Nil)(implicit ctx:NodeContextV5) ={
   for {
     timestamp    <- IO.realTime.map(_.toMillis)
     currentState <- ctx.state.get
//     value        = currentState.data.get(key)
     replyToSplit  = replyTo.split('/')
     exchange      = replyToSplit(0)
     exchangeName  = ExchangeName(exchange)
     routingKey    = RoutingKey(exchange+"."+replyToSplit(1))
     _             <- ctx.logger.info(s"KEY_NOT_FOUND $key ${arrivalTime - timestamp}")
     publisherConf = PublisherConfig(exchangeName = exchangeName,routingKey = routingKey)
     publisher     = PublisherV2(publisherConf)(ctx.rabbitContext)
     properties    = AmqpProperties(headers = Map("commandId"->StringVal("KEY_NOT_FOUND")))
     payload       = payloads.KeyNotFound(key,visitedNodes = visitedNodes,timestamp=timestamp).asJson.noSpaces
     msg           = AmqpMessage[String](payload=payload,properties=properties)
     _             <- publisher.publish(msg)
   }  yield ()


  }

  def foundKey(key:String,replyTo:String,arrivalTime:Long=0L,visitedNodes:List[String]=Nil)(implicit ctx:NodeContextV5) = {
    for {
//      REPLY TO = ECHANGE_NAME.ROUTING_KEY = load_balancer/lb-0
      timestamp <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
      value        = currentState.data.get(key)
      replyToSplit = replyTo.split('/')
      exchange     = replyToSplit(0)
      exchangeName = ExchangeName(exchange)
      routingKey   = RoutingKey(exchange+"."+replyToSplit(1))

      publisherConf = PublisherConfig(exchangeName = exchangeName,routingKey = routingKey)
      publisher     = PublisherV2(publisherConf)(ctx.rabbitContext)
//

      _            <- value match {
        case Some(value) =>   for {
          _         <- IO.unit
          properties    = AmqpProperties(headers = Map("commandId"->StringVal("KEY_FOUND")))
          payload   = payloads.KeyFound(key,value,timestamp).asJson.noSpaces
          message   = AmqpMessage[String](payload = payload,properties = properties)
          _         <- publisher.publish(message)
          _         <- ctx.logger.info(s"KEY_FOUND $key ${arrivalTime-timestamp}")
        } yield ( )
        case None => for {
          _             <- ctx.logger.error(s"KEY_NOT_FOUND $key ${visitedNodes.length+1} ${arrivalTime-timestamp}")
          properties    = AmqpProperties(headers = Map("commandId"->StringVal("KEY_NOT_FOUND")))
          payload       = payloads.KeyNotFound(key,visitedNodes = visitedNodes,timestamp=timestamp).asJson.noSpaces
          msg           = AmqpMessage[String](payload=payload,properties=properties)
          _             <- publisher.publish(msg)
        } yield ()
      }
    } yield ()
  }

  def addKey(initialTime:Long,visitedNodes:List[String],messageId:String,chordNodeId:String,key:String,value:Metadata)(implicit ctx:NodeContextV5): IO[Unit] = for {
    timestamp  <- IO.realTime.map(_.toMillis)
    nodeId     = ctx.config.nodeId
    poolId     = ctx.config.poolId
    chordNode  = Node(poolId = poolId,chordNodeId)
    publisher  = fromNodeToPublisher(chordNode)(ctx.rabbitContext)
    updatedVisitedNodes = visitedNodes.filter(_.nonEmpty) :+ nodeId
    _          <- if(chordNodeId==nodeId)  for {
      _          <- ctx.logger.info(s"ADD_KEY_ATTEMPT $messageId $key ${timestamp - initialTime}")
      exchangeName = ExchangeName(poolId)
      routingKey = RoutingKey(s"$poolId.default")
      cfg = PublisherConfig(exchangeName,routingKey)
      pub = PublisherV2(cfg)(ctx.rabbitContext)
      properties = AmqpProperties(
        headers = Map(
          "commandId"->StringVal("ADD_KEY"),
          "visitedNodes" -> StringVal(updatedVisitedNodes.mkString(","))
        ),
        messageId = messageId.some
      )
      payload    = payloads.AddKey(key,value,timestamp = timestamp).asJson.noSpaces
      message    = AmqpMessage[String](payload = payload,properties =properties)
      _ <- pub.publish(message)

    } yield ()
    else  for {
      _          <- ctx.logger.info(s"ADD_KEY_ATTEMPT $messageId $key ${timestamp - initialTime}")
      properties = AmqpProperties(
        headers = Map(
          "commandId"->StringVal("ADD_KEY"),
          "visitedNodes" -> StringVal(updatedVisitedNodes.mkString(","))
        ),
        messageId = messageId.some
      )
      payload    = payloads.AddKey(key,value,timestamp = timestamp).asJson.noSpaces
      message    = AmqpMessage[String](payload = payload,properties =properties)
      _          <- publisher.publish(message)
    } yield ()
  } yield ()

  def localAddKey(messageId:String,key:String, metadata: Metadata,initialTime:Long)(implicit ctx:NodeContextV5): IO[Unit] = for {
    timestamp   <- IO.realTime.map(_.toMillis)
    _           <- ctx.logger.info(s"ADD_KEY_SUCCESS $messageId $key ${timestamp - initialTime}")
    newMetadata = (key->metadata)
    _           <- ctx.state.update(s=>s.copy(data = s.data +  newMetadata))
  } yield ()


  def lookup(nextChordId:String,key:String,replyTo:String,messageId:String,visitedNodes:List[String]=Nil)(implicit ctx:NodeContextV5) = for {
    timestamp  <- IO.realTime.map(_.toMillis)
    nodeId     = ctx.config.nodeId
    poolId     = ctx.config.poolId
    chordNode  = Node(poolId = poolId,nextChordId)
    publisher  = fromNodeToPublisher(chordNode)(ctx.rabbitContext)
    updatedVisitedNodes = visitedNodes.filter(_.nonEmpty) :+ nodeId
    properties = AmqpProperties(
      headers = Map(
        "commandId"->StringVal("LOOKUP"),
        "visitedNodes" -> StringVal(updatedVisitedNodes.mkString(","))
      ),
      replyTo = replyTo.some,
      messageId = messageId.some
    )
    payload    = payloads.Lookup(key=key,timestamp = timestamp).asJson.noSpaces
    message    = AmqpMessage[String](payload = payload,properties =properties)
    _          <- publisher.publish(message)
  } yield ()


  def localLookup(key:String)(implicit ctx:NodeContextV5): EitherT[IO, NodeError, LookupResult] = {

    type E                       = NodeError
    val unit                     = liftFF[Unit,NodeError](IO.unit)
    val maybeCurrentState        = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
    implicit val logger          = ctx.logger
    val L                        = Logger.eitherTLogger[IO,E]
    for {
//    CURRENT_STATE
      currentState  <- maybeCurrentState
//      NODE_ID
      nodeId = ctx.config.nodeId
//     NODE_HASH
      chordNodeHash = currentState.nodeIdHash
//     FINGER_TABLE
      fingerTable   = currentState.fingerTable
//      hashes        = currentState.nodesHashes
//     NUMBER OF SLOTS
      slots         = new BigInteger(ctx.config.chordSlots.toString)
//     HASH_KEY
      hashKey       <- liftFF[BigInteger,E](Helpers.strToHashId(key,slots))
//      fingerTableV  = fingerTable.filter(x=>Helpers.isGreaterThanOrEqual(x._1,hashKey))
//    BELONGS TO ME IF HASH_KEY < NODE_HASH
//
      lastFingerTableItem = fingerTable.toList.sortBy(_.hash).lastOption
//      otherChordNode = fingerTable.filter(x=> !visitedNodes.contains(x._2) ).filter(x=>Helpers.isSuccessor(x._1,hashKey))
      filteredFingerTable = fingerTable.filter(x=>Helpers.isGreaterThanOrEqual(x.hash,hashKey))
      _ <- L.debug(filteredFingerTable.toString())
      otherChordNode = filteredFingerTable
        .toList
        .sortBy(_.hash)
        .headOption
        .getOrElse(lastFingerTableItem.get)

      successor  =  fingerTable.toList.minBy(_.hash)

      _          <- L.debug(successor.toString)
      belongsToMe   = Helpers.lowerOrEqual(chordHashId = chordNodeHash,hashKey)
//        Helpers.lowerOrEqual(chordNodeHash,otherChordNode._1)
      lookupChordResult = LookupResult(hashKey,belongsToMe = belongsToMe,belongsToOthers = otherChordNode.nodeId)

    } yield lookupChordResult

    }

    def strToHashId(x:String, slots:BigInteger): IO[BigInteger] ={
//    val slots = new BigInteger(ctx.config.chordSlots.toString)
    Stream
      .emits(x.getBytes)
      .covary[IO]
      .through(hash.sha1)
      .compile.toVector.map(x=>new BigInteger(x.toArray).mod(slots) )
  }

  def createChordNodesFromList(xs:List[String],slots:BigInteger): IO[List[ChordNode]] = xs.traverse(x=>Helpers.strToHashId(x,slots).map(id => ChordNode(x,id) ))



  def equal(chordHashId:BigInteger, id:BigInteger): Boolean ={
    id.compareTo(chordHashId) match {
      case -1 => false
      case  0 => true
      case  1 => false
    }
  }


  def lower(chordHashId:BigInteger, id:BigInteger): Boolean ={
    id.compareTo(chordHashId) match {
      case -1 => true
      case  0 => false
      case  1 => false
    }
  }
  def lowerOrEqual(chordHashId:BigInteger, id:BigInteger): Boolean ={
    chordHashId.compareTo(id) match {
      case -1 => true
      case  0 => true
      case  1 => false
    }
  }



  def isGreaterThanOrEqual(chordHashId:BigInteger, id:BigInteger): Boolean ={
    chordHashId.compareTo(id) match {
      case -1 => false
      case  0 => true
      case  1 => true
    }
  }
  def isGreaterThan(chordHashId:BigInteger, id:BigInteger): Boolean ={
    chordHashId.compareTo(id) match {
      case -1 => false
      case  0 => false
      case  1 => true
    }
  }
  def recFindSuccessors(chordHashIds:List[(String,BigInteger)], mRange2i:List[BigInteger]): List[(BigInteger, String)] ={
    mRange2i.traverse{ x=>
      chordHashIds.sortBy(_._2)
        .find{ y=>    isGreaterThan(y._2,x) }.map{
        case (chordID, hashID) =>
          (x,chordID)
      }
    }.getOrElse(Nil)
  }

  def createFingerTable(chordN:ChordNode, m:Int, slots:BigInteger, chordNodes:Map[String,ChordNode]): List[FingerTableEntry] ={
    val two            = new BigInteger("2")
    val range          = (0 until m)
    val chXs           = chordNodes.values.toList
    val fingerTable = range.map{ x=>two.pow(x).add(chordN.hash).mod(slots)}.toList
    val sortedFingerTable = fingerTable.sorted
    //         LOWES VALUE IN FT
    val lowestFTItem      = sortedFingerTable.min
    //         GET ALL GREATER NODES
    val filteredChordNodes = chXs.filter(x=>Helpers.isGreaterThan(x.hash ,lowestFTItem)).sortBy(_.hash)
    if(filteredChordNodes.isEmpty)  fingerTable.map(x=>FingerTableEntry(chXs.minBy(_.hash),x))
    else filteredChordNodes.foldLeft(List.empty[FingerTableEntry]){ (ft,chordNode) =>
      val fingerTableFiltered = fingerTable.toSet.diff(ft.map(_.value).toSet)
      val relativeLowerHashes = fingerTableFiltered.filter(x=>Helpers.lowerOrEqual(x,chordNode.hash))
      val newEntries = relativeLowerHashes.map(x=> FingerTableEntry(chordNode,x)).toList
      //            println(s"NEW_ENTRIES: ${newEntries.map(_.value).mkString(",")}")
      //            println(s"R_LOWES[${chordNode.hash}]: ${relativeLowerHashes.mkString(" // ")}")
      ft ++ newEntries
    }

//    val res   =  chordNodes.map{
//      case (str, node) =>
//        val hash = node.hash
//        (node,range.map{ x=>two.pow(x).add(hash).mod(slots)}.toList  )
//    }.map{
//      case (node, fingerTable) =>
//        //        SORTED FT
//    }
//    res.toList
//    IO.unit
//   val chordHashesIOs = chordNodeIds.traverse(strToHashId(_,slots))
//    chordHashesIOs.map(chordNodeIds zip _).map{ chordHashes=>
//      1
//    for {
//      _ <- IO.unit
//      mRange       = (0 until (m-1)).toList
//      two          = new BigInteger("2")
//      mRange2i     = mRange.map(i=>two.pow(i).add(chordHashId).mod(slots))
//      _            <- IO.println(mRange2i)
//      chordHashIds <- chordNodeIds.traverse(strToHashId(_,slots))
////        .map(_.sorted)
////      x            =
//      _  <- IO.println(chordHashIds)
////      _  <- ctx.l(chordHashIds)
//      xs = recFindSuccessors(chordNodeIds zip chordHashIds, mRange2i)
////      _  <- IO.println(xs)
//    } yield xs.toMap

  }

}
