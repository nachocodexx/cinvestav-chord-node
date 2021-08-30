import cats.Order
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.{ExchangeName, ExchangeType, RoutingKey}
import fs2.Stream
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.Chord.ChordNodeWithDistance
import mx.cinvestav.Main.config
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.payloads
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import mx.cinvestav.{Chord, ChordNodeInfo, CommandId, Helpers}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.concurrent.duration._
import scala.language.postfixOps
import fs2.{Stream, hash, text}
import mx.cinvestav.Declarations.{ChordNode, DefaultConfigV5, FingerTableEntry}
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, RabbitMQContext}

import java.math.BigInteger
import java.util.UUID
class BasicSpec extends munit.CatsEffectSuite {
  implicit val config: DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  case class MyKey(id:String,replicaIndex:Int)


  test("Segement") {
    val start   = new BigInteger("359")
    val end     = new BigInteger("50")
    val segment = ((start.intValue() until 360 ).toList ++ (0 until end.intValue()+1).toList).sorted
    val segment2 = Helpers.generateSegment(start,end,360)
//    val segment2 = generateSegment(new BigInteger("1"),new BigInteger("10"),360)
    println(segment)
    println(segment2)
  }

  test("Find successor"){



//    val currentChordNode = ChordNode("ch-0",new BigInteger("50"),None)
//    val currentChordNode = ChordNode("ch-2",new BigInteger("72"),None)
    val currentChordNode = ChordNode("ch-1",new BigInteger("359"),None)
//    val seventyTwo = new BigInteger("72")
//    val threeHundred59 = new BigInteger("359")
    val fifty = new BigInteger("50")
    val fingerTable = List(

      FingerTableEntry(ChordNode("ch-0",fifty,None), new BigInteger("0") ),
      FingerTableEntry(ChordNode("ch-0",fifty,None), new BigInteger("1") ),
      FingerTableEntry(ChordNode("ch-0",fifty,None), new BigInteger("3") )
//
//      FingerTableEntry(ChordNode("ch-1",threeHundred59,None), new BigInteger("73") ),
//      FingerTableEntry(ChordNode("ch-1",threeHundred59,None), new BigInteger("74") ),
//      FingerTableEntry(ChordNode("ch-1",threeHundred59,None), new BigInteger("76") )
//      ______________________________________
//      FingerTableEntry(ChordNode("ch-2",seventyTwo,None), new BigInteger("51") ),
//      FingerTableEntry(ChordNode("ch-2",seventyTwo,None), new BigInteger("52") ),
//      FingerTableEntry(ChordNode("ch-2",seventyTwo,None), new BigInteger("54") )
    )
    val res = Helpers.findSuccessor(360,currentChordNode,new BigInteger("73"),fingerTable)
    println(res)
  }

  test("Chord"){
    val m      = 3
    val slots  = new BigInteger("360")
//    val one
    //  "ch-EiIRBUKfQCIJ",
    //  "ch-FVZ9e4CteXht",
    //  "ch-Jww5yzGJzYWe",
    //  "ch-oMThk35aOzfG",
    //  "ch-evkjQPHZevEa"

//    val chIds  = List("ch-EiIRBUKfQCIJ","ch-FVZ9e4CteXht","ch-Jww5yzGJzYWe","ch-oMThk35aOzfG","ch-evkjQPHZevEa")
    val chIds  = List("ch-0","ch-1","ch-2")
    val chIdsS = Stream.emits(chIds)
    val chHashes = chIdsS.covary[IO].evalMap(x=>
      Stream.emits(x.getBytes)
        .covary[IO]
        .through(hash.sha1)
        .compile
        .to(Array)
        .map(x=>new BigInteger(x))
        .map(_.mod( slots))
    )
//      .debug(x=>s"CHORD_HASH $x")
    val range  = (0 until m)
    val two    = new BigInteger("2")
//      slots.subtract(BigInteger.ONE ).intValue()

    (chHashes zip chIdsS).map{
      case (hash,chId)=>
      val fingerTable = range.map{ x=>
        two.pow(x).add(hash).mod(slots)
      }.toList
      ( ChordNode(nodeId = chId, hash=hash,None),fingerTable )
    }.debug(x=> s"CHORD_HASH ${x._1}")
      .compile
      .toVector
      .map{ x=>x.flatMap{
        case (node, fingerTable) =>
//         CHORD NODES
          val chordNodes        = x.map(_._1).toList
//        SORTED FT
          val sortedFingerTable = fingerTable.sorted
//         LOWES VALUE IN FT
          val lowestFTItem      = sortedFingerTable.min
//         GET ALL GREATER NODES
          val filteredChordNodes = chordNodes.filter(x=>Helpers.isGreaterThan(x.hash ,lowestFTItem)).sortBy(_.hash)
//
          if(filteredChordNodes.isEmpty)  fingerTable.map(x=>FingerTableEntry(chordNodes.minBy(_.hash),x))
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
      .flatMap(fts=>IO.println(fts.mkString(" // \n")+"\n")  )

  }

  test("Basic: Keys"){
    val data:Map[MyKey,String] = Map(
      MyKey("f00",0) -> "{\"nodes\":[\"sn0\",\"sn1\"]}",
      MyKey("f00",1) -> "{\"nodes\":[\"sn0\",\"sn1\"]}",
    )
    val ht:Map[String,Int] = Map.empty[String,Int]
    val h2 = ht.updatedWith("1")(_.map(_+1).orElse(Some(1)))
    val n = new BigInteger("1000")
    val m = new BigInteger("100")
    println(n.compareTo(m))
//    println(h2)
  }


  test("Basic:Hash"){
    val fileId     = "f701a41d-13d2-486b-bb17-c11c8bd907b5" // 5 - 2 - 1 - 5 - 3 - 4 -5
    val totalNodes = 3
    val m          = 3*totalNodes
    val hashFileId = Stream.emits("2".getBytes)
      .covary[IO]
      .through(hash.sha1)

    hashFileId
      .compile.to(Array)
      .map(x=>new BigInteger(x))
      .map(_.mod(new BigInteger(m.toString)))
      .flatMap(IO.println)
  }

  test("Basic: 00"){
    val app = Stream
      .iterate(0)(_+1)
      .covary[IO]
      .take(5)
      .filter(_!=3)
      .dropWhile{nId =>
        if(nId==4) !Chord.checkIfBelongs(10,nId,step=6)
        else !Chord.checkIfBelongs(10,nId,step=3)
      }
      .map(_%5)
//      .debug()

    val chordNodeId = app.compile.toList.map(_.headOption)
    chordNodeId.flatMap(IO.println)
  }

  test("Basic: 01"){
    val info = Chord.buildChordInfoById(2,3)
    println(info)
  }

  test("Basic: 02"){
//    val xs = List(false, false, true).find(identity)
    val idChordNode        = ChordNodeInfo(-1,15,15)
    val chordNodeId        = 1
    val numbersOfNeighbors = 2
    val totalOfNodes       = 6
    val successors         = Chord.seqChordInfo(keyIdsPerNode = 3,numbersOfNeighbors)(totalOfNodes,chordNodeId+_)
    val predecessors       = Chord.seqChordInfo(keyIdsPerNode = 3,numbersOfNeighbors)(totalOfNodes,chordNodeId-_)
    val fingerTable        = Chord.seqChordInfo(keyIdsPerNode = 3, totalOfNodes)(totalOfNodes,
      incrementFn = (x => chordNodeId + math.pow(2, x - 1).toInt)).distinct
    val data= (fingerTable.filter(_.chordId!=chordNodeId)++successors++predecessors).distinct
//    println(fingerTable)
    val res= Chord.getRelativeDistance(idChordNode,data)
    println(res.sorted)
//        println(successors)
//        println(predecessors)

//    val xs = ( 1 until (numbersOfNeighbors+1))
//      .map(chordNodeId+_)
//      .map(_+6)
//      .map(_%6)
//    println(xs)
//    val predecessors = Chord.seqChordInfo(3,chordNodeId,numbersOfNeighbors)(6,x=>chordNodeId-(x+1))
//    val from = 0
//    val to = 2
//    val xs = (from until to).map(x=>  from - (x+1) )
//      .map(_+6)
//      .map(_%6)
//    println(xs)
//    println( ((0-2)+6)%6 )
//    val res = successors.foldl(List.empty[ChordNodeWithDistance]){(xs,chordNode)=>
//      val distance = Chord.calculateDistance(idChordNode,chordNode)
//      xs :+ ChordNodeWithDistance(chordNode,distance)
//    }


//    val x = ChordNodeWithDistance(ChordNodeInfo(1,10,2),6)
//    val y = ChordNodeWithDistance(idChordNode,5)
//    val re = Order[ChordNodeWithDistance].gt(x,y)
//    println(re)
//    val results = successors.map(Chord.getDistances(idChordNode,_))
//    val results0 = predecessors.map(Chord.getDistances(idChordNode,_))
//    println(res)
//    println(predecessors)
//    println(results0)
  }

  test("Example"){
    RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils =>
      val poolId           = "pool-xxxx"
      val key              = "f_01"
      val chordId          = "ch-4"
      val experimentId     = 100
      val addKeyPayload    = payloads.AddKey(id="op_00",experimentId = experimentId,key=key,value="sn-00")
      val addKeyCmd        = CommandData[Json](CommandId.ADD_KEY,addKeyPayload.asJson)
      val lookupKeyPayload = payloads.Lookup(id = "op_00",key=key,replyTo = "testing",exchangeName = poolId,experimentId = experimentId,lookupTrace = Nil)
      val lookupCmd         = CommandData[Json](CommandId.LOOKUP,lookupKeyPayload.asJson)
      for {
        _            <- Logger[IO].info("INIT!")
        chordNodePub <- utils.createPublisher(poolId,s"$poolId.global.chord")
        _ <- utils.createQueue(
          queueName = "testing",
          exchangeName =poolId ,
          exchangeType =ExchangeType.Topic ,
          routingKey = "testing"
        )
//        _ <- chordNodePub(addKeyCmd.asJson.noSpaces)
//        _ <- IO.sleep(2 seconds)
        _ <- chordNodePub(lookupCmd.asJson.noSpaces)
        _ <- utils.consumeJson("testing")
          .evalMap{ data=>
            Logger[IO].info(data.toString)
          }
          .compile.drain

      } yield ()
    }
  }

}
