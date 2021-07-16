package mx.cinvestav.handlers

import cats.effect.IO
import io.circe.{DecodingFailure, Json}
import mx.cinvestav.Main.NodeContext
import mx.cinvestav.payloads.Payloads.Lock
import io.circe.syntax._,io.circe.generic.auto._
//import mx.cinvestav.commons.payloads.Lookup
import mx.cinvestav.utils.Command

class LockHandler(command: Command[Json])(implicit ctx:NodeContext[IO]) extends CommandHandler [IO,Lock]{
  override def handleLeft(df: DecodingFailure): IO[Unit] = ctx.logger.error(df.getMessage())

  override def handleRight(payload: Lock): IO[Unit] = for {
    _<- IO.unit
  } yield ()

  override def handle(): IO[Unit] = handler(command.payload.as[Lock])
}
object LockHandler{
  def apply(command: Command[Json])(implicit ctx:NodeContext[IO]): IO[Unit] = new LockHandler(command).handle()
}
