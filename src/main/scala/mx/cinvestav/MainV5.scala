package mx.cinvestav

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, ExchangeName, QueueName, RoutingKey}
import mx.cinvestav.Declarations.{ChordNode, DefaultConfigV5, NodeContextV5, NodeStateV5}
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2.{Acker, Exchange, MessageQueue, RabbitMQContext, RawAcker}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import fs2.{Stream, hash}

import java.math.BigInteger
import java.net.InetAddress

object MainV5 extends IOApp{
  implicit val config:DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  implicit val rabbitMQConfig: Fs2RabbitConfig = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]


  def consume(_acker:RawAcker, envelope:AmqpEnvelope[String])(implicit ctx:NodeContextV5): IO[Unit] = {
      val maybeCommandId = envelope.properties.headers.get("commandId")
      implicit val acker: Acker = Acker(_acker)
      implicit val _envelope = envelope

     maybeCommandId match {
        case Some(commandId) => commandId match {
          case StringVal(value) if value =="LOOKUP" =>  CommandHandlers.lookup
          case StringVal(value) if value =="ADD_KEY" =>  CommandHandlers.addKey()
//          case StringVal(value) if value == "UPDATE_CHUNK_LOCATION" => CommandHandlers.updateChunkLocation()
          case StringVal(value) =>
            acker.reject(envelope.deliveryTag) *> ctx.logger.error(s"NO_COMMAND_HANDLER[$value]")
        }
        case None => for {
          _ <- acker.reject(envelope.deliveryTag)
          _ <- ctx.logger.debug("NO COMMAND_ID PROVIDED")
        } yield ()
      }
  }

  def mainProgram(mainQueue:QueueName,queueName: QueueName)(implicit ctx:NodeContextV5): IO[Unit] = {
    val client     = ctx.rabbitContext.client
    val connection = ctx.rabbitContext.connection
    client.createChannel(conn = connection).use{ implicit channel=>
      for {
        (_acker,consumer) <- client.createAckerConsumer(queueName = queueName)
        (_acker2,consumer2) <- client.createAckerConsumer(queueName = mainQueue)
        stream =  consumer.evalMap(consume(_acker,_)).concurrently(consumer2.evalMap(consume(_acker2,_)))
        _ <- stream.compile.drain
      }  yield ( )
    }
  }
  //
  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig) { implicit client =>
      client.createConnection.use{ implicit connection=>
        for {
          _               <- Logger[IO].debug(s"CHORD[${config.nodeId}]")
          implicit0(rabbitContext:RabbitMQContext)   <- IO.pure(RabbitMQContext(client=client,connection=connection))
          poolId          = config.poolId
          nodeId          = config.nodeId
          //        ____________________________________________________________________________
          exchangeName  = ExchangeName(s"$poolId")
          queueName     = QueueName(s"$nodeId")
          routingKey    = RoutingKey(s"$poolId.$nodeId")
          //         ______________________________________________________________-
          exchange      <- Exchange.topic(exchangeName = exchangeName)
          mainQueueName = QueueName(s"$poolId")
          chordQueue   <- MessageQueue.createThenBind(
            queueName =mainQueueName,
            exchangeName = exchangeName,
            routingKey =  RoutingKey(s"$poolId.default") ,
          )
          queue         <- MessageQueue.createThenBind(
            queueName = queueName,
            exchangeName = exchangeName,
            routingKey = routingKey
          )
          //         __________________________________________________________

          maxSlots      = new BigInteger(config.chordSlots.toString)
          nodeIdHash    <- Helpers.strToHashId(config.nodeId,maxSlots)
          nodeId        = config.nodeId
          chordNode     = ChordNode(nodeId,nodeIdHash)
          nodeIds       = config.chordNodes.filter(_!= nodeId)
          hashes <-  Helpers.createChordNodesFromList(nodeIds,maxSlots).map(nodeIds zip _ ).map(_.toMap)
//          nodesToHashes <- Helpers.createChordNodesFromList(config.chordNodes.filter(_!=config.nodeId),maxSlots)
          fingerTable = Helpers.createFingerTable(
            chordN     = chordNode,
            m          = config.chordM,
            slots      = maxSlots,
            chordNodes = hashes
          )
          _ <- Logger[IO].debug(s"CHORD_HASH_ID $nodeIdHash")
           _ <- Logger[IO].debug(fingerTable.toString)
          initState       = NodeStateV5(
            ip          = InetAddress.getLocalHost.getHostAddress,
            nodeIdHash  = nodeIdHash,
            fingerTable =  Nil,
//              fingerTable,
            nodesHashes =  hashes
          )
          state           <- IO.ref(initState)
          context         = NodeContextV5(state=state,rabbitContext = rabbitContext,logger = unsafeLogger,config=config)
          _               <- mainProgram(mainQueue =mainQueueName ,queueName = queueName)(ctx=context)
          //          _             <- HttpServer.run()(ctx=context)
        } yield ()
      }
    }.as(ExitCode.Success)
  }

}
