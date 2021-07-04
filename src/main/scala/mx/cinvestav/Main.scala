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
import fs2.hash
import mx.cinvestav.CommandId
import mx.cinvestav.handlers.{AddKeyHandler, LookupHandler}

object Main extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val rabbitMQConfig: Fs2RabbitConfig  = dynamicRabbitMQConfig(config.rabbitmq)
  case class NodeContext[F[_]](config: DefaultConfig,logger: Logger[F],utils: RabbitMQUtils[F],state:Ref[IO,NodeState])
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def program(queueName:String)(implicit ctx:NodeContext[IO]) =
    ctx.utils.consumeJson(queueName = queueName).evalMap { command =>
      command.commandId match {
        case CommandId.ADD_KEY => AddKeyHandler(command)
        case CommandId.LOOKUP => LookupHandler(command)
        case _ => ctx.logger.error("COMMAND NOT FOUND")
      }
    }.compile.drain

  override def run(args: List[String]): IO[ExitCode] = RabbitMQUtils.init[IO](rabbitMQConfig){ implicit utils =>
    for {
      _             <- Logger[IO].debug(s"CHORD NODE[${config.nodeId}] is up and running 🚀")
      chordInfo     = Chord.buildChordInfoById(config.chordId,config.chordStep)
      initState     = NodeState(
        chordInfo    = chordInfo,
        fingerTable  = Map.empty[Int,ChordNodeInfo],
        successor    = Chord.buildChordInfoById(
          chordId = ((config.chordId+1)+config.totalOfNodes) % config.totalOfNodes,
          step    = config.chordStep
        ),
        predecessor     = Chord.buildChordInfoById(
          chordId = ((config.chordId-1)+config.totalOfNodes) % config.totalOfNodes,
          step    = config.chordStep
        ),
        totalOfNodes =  config.totalOfNodes,
        data         = Map.empty[String,String]
      )
      state         <- IO.ref(initState)
      ctx           = NodeContext(config= config,logger = unsafeLogger,utils=utils,state)
      _             <- ctx.logger.info(initState.toString)
      routingKey    = s"${ctx.config.poolId}.${Chord.chordIdWithPrefix(config.chordId)}.default"
      mainQueueName = s"${ctx.config.poolId}-${Chord.chordIdWithPrefix(config.chordId)}"
      _             <- ctx.utils.createQueue(mainQueueName,config.poolId,ExchangeType.Topic,routingKey)
     _ <- program(queueName = mainQueueName)(ctx=ctx)
    } yield()
  }.as(ExitCode.Success)
}
