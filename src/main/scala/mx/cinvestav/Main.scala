package mx.cinvestav

import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Ref}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.ExchangeType
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.RabbitMQUtils.dynamicRabbitMQConfig
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig._
import pureconfig.generic.auto._
import fs2.{Stream, hash}
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.handlers.{AddKeyHandler, LockHandler, LookupHandler}

import java.math.BigInteger

object Main extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig  = dynamicRabbitMQConfig(config.rabbitmq)
  case class NodeContext[F[_]](config: DefaultConfig,logger: Logger[F],utils: RabbitMQUtils[F],state:Ref[IO,NodeState])
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def program(queueName:String)(implicit ctx:NodeContext[IO]): IO[Unit] =
    ctx.utils.consumeJson(queueName = queueName).evalMap { command =>
      command.commandId match {
        case Identifiers.ADD_KEY => AddKeyHandler(command)
        case Identifiers.LOOKUP  => LookupHandler(command)
        case CommandId.LOCK    => LockHandler(command)
        case _                 => ctx.logger.error("COMMAND NOT FOUND")
      }
    }.compile.drain


  override def run(args: List[String]): IO[ExitCode] = RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils =>
    for {
      _             <- Logger[IO].debug(s"CHORD NODE[${config.nodeId}] is up and running 🚀")
      idHashValue   <- Stream.emits(config.nodeId.getBytes).covary[IO].through(hash.sha1).compile.to(Array)
      chordHashId   = new BigInteger(idHashValue).mod(new BigInteger(config.chordRingLength.toString))
      _             <- Logger[IO].debug(chordHashId.toString)
      chordInfo     = Chord.buildChordInfoById(config.chordId,config.keysPerNode)
      successors    = Chord.seqChordInfo(config.keysPerNode,config.numberOfSuccessors)(config.totalOfNodes,config.chordId+_)
      predecessors  = Chord.seqChordInfo(config.keysPerNode,config.numberOfPredecessors)(config.totalOfNodes,config.chordId-_)
      fingerTable   = Chord.seqChordInfo(config.keysPerNode,config.totalOfNodes-1)(config.totalOfNodes,
        incrementFn= (x=>config.chordId+math.pow(2,x-1).toInt)
      ).filter(_.chordId!=config.chordId).distinct
      initState       = NodeState(
        chordHashId   = chordHashId ,
        chordInfos    = chordInfo::Nil,
        successors    = successors,
        predecessors  = predecessors,
        chordsData    = (successors++predecessors++fingerTable).distinct,
        fingerTable   = fingerTable,
//          Map.empty[Int,ChordNodeInfo],
        totalOfNodes  = config.totalOfNodes,
        data          = Map.empty[String,String],
        totalOfKeyIds = config.totalOfNodes*config.keysPerNode,
        fingerTableV2 = Map.empty[BigInteger,BigInteger]
      )
      state         <- IO.ref(initState)
      ctx           = NodeContext(config= config,logger = unsafeLogger,utils=utils,state)
      _             <- ctx.logger.info(initState.toString)
      globalChordQueue = s"${ctx.config.poolId}-chord"
      globalChordRk = s"${ctx.config.poolId}.global.chord"
      _ <- ctx.utils.createQueue(
        queueName = globalChordQueue,
        exchangeName = config.poolId,
        exchangeType = ExchangeType.Topic,
        routingKey = globalChordRk
      )
      _ <- program(queueName = globalChordQueue)(ctx).start
      directRoutingKey    = s"${ctx.config.poolId}.chord.${Chord.chordIdWithPrefix(config.chordId)}"
      directQueueName = s"${ctx.config.poolId}-${Chord.chordIdWithPrefix(config.chordId)}"
      _             <- ctx.utils.createQueue(directQueueName,config.poolId,ExchangeType.Topic,directRoutingKey)
      _  <- program(queueName = directQueueName)(ctx=ctx)
    } yield()
  }.as(ExitCode.Success)
}
