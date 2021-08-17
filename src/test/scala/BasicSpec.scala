import cats.Order
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.ExchangeType
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
import mx.cinvestav.{Chord, ChordNodeInfo, CommandId}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import scala.concurrent.duration._
import scala.language.postfixOps
import fs2.{Stream, hash, text}

import java.math.BigInteger
import java.util.UUID
class BasicSpec extends munit.CatsEffectSuite {
  implicit val config: DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig  = dynamicRabbitMQConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  case class MyKey(id:String,replicaIndex:Int)
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
