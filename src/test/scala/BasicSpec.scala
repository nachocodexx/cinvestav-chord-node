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
import mx.cinvestav.payloads.Payloads
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import mx.cinvestav.{Chord, ChordNodeInfo, CommandId}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.ConfigSource

import java.util.UUID
class BasicSpec extends munit.CatsEffectSuite {
  implicit val config: DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig  = dynamicRabbitMQConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

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
    val idChordNode = ChordNodeInfo(-1,10,10)
    val chordNodeId = 2
    val numbersOfNeighbors = 6
    val totalOfNodes = 16
    val successors = Chord.seqChordInfo(3,numbersOfNeighbors)(totalOfNodes,chordNodeId+_)
    val predecessors = Chord.seqChordInfo(keyIdsPerNode = 3,numbersOfNeighbors)(totalOfNodes,chordNodeId-_)
    val fingerTable = Chord.seqChordInfo(keyIdsPerNode = 3,numbersOfNeighbors)(totalOfNodes,
      incrementFn= (x=>chordNodeId+math.pow(2,x-1).toInt))
    println(fingerTable.filter(_.chordId!=2))
    //    println(successors)
    //    println(predecessors)

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
      val key           = "814efcac-e35e-41c8-bee6-0d0c49fbb47d"
      val chordId       = "ch-4"
      val addKeyPayload = Payloads.AddKey(key,"sn-00")
      val addKeyCmd     = CommandData[Json](CommandId.ADD_KEY,addKeyPayload.asJson)
      val lookupKeyPayload= Payloads.Lookup(key,"testing","pool-xxxx")
      val lookupCmd = CommandData[Json](CommandId.LOOKUP,lookupKeyPayload.asJson)
      val poolId = "pool-xxxx"
      for {
        _            <- Logger[IO].info("INIT!")
        chordNodePub <- utils.createPublisher(poolId,s"$poolId.global.chord")
//        _ <- utils.createQueue(
//          queueName = "testing",
//          exchangeName ="pool-xxxx" ,
//          exchangeType =ExchangeType.Topic ,
//          routingKey = "testing"
//        )
        _ <- chordNodePub(addKeyCmd.asJson.noSpaces)
//        _ <- chordNodePub(lookupCmd.asJson.noSpaces)
//        _ <- utils.consumeJson("testing")
//          .evalMap{ data=>
//            Logger[IO].info(data.toString)
//          }
//          .compile.drain

      } yield ()
    }
  }

}
