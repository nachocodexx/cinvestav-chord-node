package mx.cinvestav

import cats.data.EitherT
import cats.implicits._
import cats.effect.IO
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.{LongVal, StringVal}
import dev.profunktor.fs2rabbit.model.{AmqpMessage, AmqpProperties, ExchangeName, RoutingKey}
import fs2.{Stream, hash}
import mx.cinvestav.Declarations.{ChordNode, DefaultConfigV5, FingerTableEntry, NodeContextV5, NodeError, NodeStateV5, LookupResult => LRes}
import mx.cinvestav.commons.types.{LookupResult, Metadata}
//import mx.cinvestav.Declarations
import mx.cinvestav.commons.liftFF
import org.typelevel.log4cats.Logger

import java.math.BigInteger
import mx.cinvestav.commons.nodes.Node
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, RabbitMQContext, fromNodeToPublisher}
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.payloads.{v2 => payloads}
import mx.cinvestav.payloads.Payloads
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
//      REPLY TO = ECHANGE_NAME/SEGMENT_ROUTING_KEY = load_balancer/lb-0
      timestamp    <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
      value        = currentState.data.get(key)
//     Provisional
      replyToSplit = replyTo.split('/')
      exchange     = replyToSplit.headOption.getOrElse("test")
      exchangeName = ExchangeName(exchange)
      rk           = replyToSplit.tail.headOption.getOrElse("test")
      routingKey   = RoutingKey(exchange+"."+replyToSplit(1))
//
      publisherConf = PublisherConfig(exchangeName = exchangeName,routingKey = routingKey)
      publisher     = PublisherV2(publisherConf)(ctx.rabbitContext)
//

      _            <- value match {
        case Some(value) =>   for {
          _          <- IO.unit
          properties = AmqpProperties(headers = Map("commandId"->StringVal("KEY_FOUND")))
          payload    = payloads.KeyFound(key,value,timestamp).asJson.noSpaces
          message    = AmqpMessage[String](payload = payload,properties = properties)
          _          <- publisher.publish(message).handleErrorWith{ t=>
            ctx.logger.error(t.getMessage)
          }
          _          <- ctx.logger.info(s"KEY_FOUND $key ${arrivalTime-timestamp}")
        } yield ( )
        case None => for {
          _             <- ctx.logger.error(s"KEY_NOT_FOUND $key ${visitedNodes.length+1} ${arrivalTime-timestamp}")
          properties    = AmqpProperties(headers = Map("commandId"->StringVal("KEY_NOT_FOUND")))
          payload       = payloads.KeyNotFound(key,visitedNodes = visitedNodes,timestamp=timestamp).asJson.noSpaces
          msg           = AmqpMessage[String](payload=payload,properties=properties)
          _             <- publisher.publish(msg).handleErrorWith{ t=>
            ctx.logger.error(t.getMessage)
          }
        } yield ()
      }
    } yield ()
  }

  def addKey(initialTime:Long,visitedNodes:List[String],messageId:String,chordNode:ChordNode,key:String,value:Metadata)(implicit ctx:NodeContextV5): IO[Unit] = for {
    timestamp  <- IO.realTime.map(_.toMillis)
    nodeId     = ctx.config.nodeId
    poolId     = ctx.config.poolId
//    chordNode  = Node(poolId = poolId,chordNodeId)
//    publisher  = fromNodeToPublisher(chordNode)(ctx.rabbitContext)
    updatedVisitedNodes = visitedNodes.filter(_.nonEmpty) :+ nodeId
//    _          <- if(chordNodeId==nodeId)  for {
      _          <- ctx.logger.info(s"ADD_KEY_ATTEMPT $messageId $key ${timestamp - initialTime}")
//      exchangeName = ExchangeName(poolId)
//      routingKey = RoutingKey(s"$poolId.default")
//      cfg = PublisherConfig(exchangeName,routingKey)
//      pub = PublisherV2(cfg)(ctx.rabbitContext)
      properties = AmqpProperties(
        headers = Map(
          "commandId"->StringVal("ADD_KEY"),
          "visitedNodes" -> StringVal(updatedVisitedNodes.mkString(",")),
          "timestamp" -> LongVal(initialTime)
        ),
        messageId = messageId.some
      )
      payload    = payloads.AddKey(key,value,timestamp = timestamp).asJson.noSpaces
      message    = AmqpMessage[String](payload = payload,properties =properties)
      x <- chordNode.publisher.traverse(_.publish(message))
//
//    } yield ()
//    else  for {
//      _          <- ctx.logger.info(s"ADD_KEY_ATTEMPT $messageId $key ${timestamp - initialTime}")
//      properties = AmqpProperties(
//        headers = Map(
//          "commandId"->StringVal("ADD_KEY"),
//          "visitedNodes" -> StringVal(updatedVisitedNodes.mkString(","))
//        ),
//        messageId = messageId.some
//      )
//      payload    = payloads.AddKey(key,value,timestamp = timestamp).asJson.noSpaces
//      message    = AmqpMessage[String](payload = payload,properties =properties)
//      _          <- publisher.publish(message)
//    } yield ()
  } yield ()

  def localAddKey(messageId:String,key:String, metadata: Metadata,initialTime:Long)(implicit ctx:NodeContextV5): IO[Unit] = for {
    timestamp   <- IO.realTime.map(_.toMillis)
    _           <- ctx.logger.info(s"ADD_KEY_SUCCESS $messageId $key ${timestamp - initialTime}")
    newMetadata = (key->metadata)
    _           <- ctx.state.update(s=>s.copy(data = s.data +  newMetadata))
  } yield ()


  def lookup(nextChordNode:ChordNode,key:String,replyTo:String,messageId:String,visitedNodes:List[String]=Nil)(implicit ctx:NodeContextV5) = for {
    timestamp  <- IO.realTime.map(_.toMillis)
    nodeId     = ctx.config.nodeId
//    poolId     = ctx.config.poolId
//    chordNode  = Node(poolId = poolId,nextChordId)
//    publisher  = fromNodeToPublisher(chordNode)(ctx.rabbitContext)
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
    _          <- nextChordNode.publisher.traverse(_.publish(message))
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
      chordNodeHash = currentState.chordNode.hash
//     FINGER_TABLE
      fingerTable   = currentState.fingerTable
//      hashes        = currentState.nodesHashes
//     NUMBER OF SLOTS
      slots         = new BigInteger(ctx.config.chordSlots.toString)
//     HASH_KEY
      hashKey       <- liftFF[BigInteger,E](Helpers.strToHash(key,slots))
//      fingerTableV  = fingerTable.filter(x=>Helpers.isGreaterThanOrEqual(x._1,hashKey))
//    BELONGS TO ME IF HASH_KEY < NODE_HASH
//
      sortedFT = fingerTable.toList.sortBy(_.chordNode.hash)
      lastFingerTableItem = sortedFT.lastOption
//      otherChordNode = fingerTable.filter(x=> !visitedNodes.contains(x._2) ).filter(x=>Helpers.isSuccessor(x._1,hashKey))
      filteredFingerTable = fingerTable.filter(x=>Helpers.isGreaterThanOrEqual(x.chordNode.hash,hashKey))
      _ <- L.debug(filteredFingerTable.toString())
      otherChordNode = filteredFingerTable
        .toList
        .sortBy(_.chordNode.hash)
        .headOption
        .getOrElse(lastFingerTableItem.get)

      successor  =  fingerTable.toList.minBy(_.chordNode.hash)

      _          <- L.debug(successor.toString)
      belongsToMe   = Helpers.lowerOrEqual(x = chordNodeHash,hashKey)
//        Helpers.lowerOrEqual(chordNodeHash,otherChordNode._1)
      lookupChordResult = LookupResult(hashKey,belongsToMe = belongsToMe,belongsToOthers = otherChordNode.chordNode.nodeId)

    } yield lookupChordResult

    }


//
  def closestPrecedingNode(chordNode: ChordNode,key:BigInteger,fingerTable:List[FingerTableEntry])={
    fingerTable.sortBy(_.value).reverse.filter{ x=>
      val  fTEntryValue = x.value
      Helpers.isGreaterThan(fTEntryValue,chordNode.hash) && Helpers.isLowerThan(fTEntryValue,key)
    }.map(_.chordNode).sortBy(_.hash).headOption.getOrElse(chordNode)
  }
  //      val properties = AmqpProperties(headers = Map("commandId" -> StringVal("LOOKUP")) )
  //      val payload    = payloads.Lookup
  //      val message = AmqpMessage[String](payload ="",properties = properties)
  //      successor.chordNode.publisher.traverse(_.publish())
  def findSuccessor(mSlots:Int,n:ChordNode,key:BigInteger,fingerTable:List[FingerTableEntry])= {
    val successor         = fingerTable.minBy(_.value)
    val successorNodeHash = successor.chordNode.hash
    val segment           = Helpers.generateSegment(n.hash,successorNodeHash,mSlots).map(x=>new BigInteger(x.toString)).toSet
//    BELONGS TO THE SUCCESSOR
    if(segment.contains(key)) LRes(key,successor.chordNode)
    else {
      val newN = Helpers.closestPrecedingNode(n,key,fingerTable)
//    Belongs to this node
      if(newN.nodeId == n.nodeId)  LRes(key,n,true)
//     Belongs to another node
      else LRes(key,newN,false)

    }
  }
  def generateSegment(x:BigInteger,y:BigInteger,m:Int): List[Int] = {
      if(Helpers.isLowerThan(x,y))
        ((x.intValue() until y.intValue()+1)).toList
      else
        ((0 until y.intValue()+1).toList ++ (x.intValue() until m)  .toList  )  .sorted
    }
//
    def strToHash(x:String, slots:BigInteger): IO[BigInteger] ={
//    val slots = new BigInteger(ctx.config.chordSlots.toString)
    Stream
      .emits(x.getBytes)
      .covary[IO]
      .through(hash.sha1)
      .compile.toVector.map(x=>new BigInteger(x.toArray).mod(slots) )
  }

  def createChordNodesFromList(xs:List[String],slots:BigInteger)(implicit config:DefaultConfigV5, rabbitContext:RabbitMQContext): IO[List[ChordNode]] =
    xs.traverse{ x=>
      val poolId          = config.poolId
      val rk              = RoutingKey(s"$poolId.$x")
      val exchangeName    = ExchangeName(config.poolId)
      val publisherConfig = PublisherConfig(exchangeName = exchangeName,routingKey =rk)
//    ________________________________________________________________________________________
      val publisher = PublisherV2.create(x,publisherConfig)
      Helpers.strToHash(x,slots).map(id => ChordNode(x,id,publisher = publisher.some) )
    }



  def equal(x:BigInteger, id:BigInteger): Boolean ={
    x.compareTo(id) match {
      case -1 => false
      case  0 => true
      case  1 => false
    }
  }


  def isLowerThan(x:BigInteger, id:BigInteger): Boolean ={
    x.compareTo(id) match {
      case -1 => true
      case  0 => false
      case  1 => false
    }
  }
  def lowerOrEqual(x:BigInteger, id:BigInteger): Boolean ={
    x.compareTo(id) match {
      case -1 => true
      case  0 => true
      case  1 => false
    }
  }



  def isGreaterThanOrEqual(x:BigInteger, id:BigInteger): Boolean ={
    x.compareTo(id) match {
      case -1 => false
      case  0 => true
      case  1 => true
    }
  }
  def isGreaterThan(x:BigInteger, id:BigInteger): Boolean ={
    x.compareTo(id) match {
      case -1 => false
      case  0 => false
      case  1 => true
    }
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

  }

}
