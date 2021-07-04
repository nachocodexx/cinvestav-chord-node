package mx.cinvestav.handlers
import cats.implicits._
import cats.effect._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.CommandId
import mx.cinvestav.Main.{NodeContext, config}
import mx.cinvestav.payloads.Payloads.Lookup
import mx.cinvestav.utils.Command

import fs2.Stream,fs2.hash
class LookupHandler(command: Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler [IO,Lookup]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: Lookup): IO[Unit] = for {
     _      <- ctx.logger.debug(CommandId.LOOKUP+s" ${payload.key} ${payload.replyTo}")
    keyHash = Stream.emits(payload.key.getBytes).through(hash.sha1).toVector
    _       <- ctx.logger.info(keyHash.mkString(","))
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Lookup])
}

object LookupHandler {
  def apply(command:Command[Json])(implicit ctx:NodeContext[IO]) =
    new LookupHandler(command=command).handle()
}
