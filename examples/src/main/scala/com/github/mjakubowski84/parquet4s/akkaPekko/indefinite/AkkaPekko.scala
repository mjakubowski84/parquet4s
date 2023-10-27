package com.github.mjakubowski84.parquet4s.akkaPekko.indefinite

import com.github.mjakubowski84.parquet4s.ScalaCompat.actor.{ActorSystem, CoordinatedShutdown}
import com.github.mjakubowski84.parquet4s.ScalaCompat.pattern.AskSupport

import scala.concurrent.ExecutionContext

trait AkkaPekko extends AskSupport {

  this: Logger =>

  implicit lazy val system: ActorSystem           = ActorSystem()
  implicit def executionContext: ExecutionContext = system.dispatcher
  val coordinatedShutdown: CoordinatedShutdown    = CoordinatedShutdown(system)

}
