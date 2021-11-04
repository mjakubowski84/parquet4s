package com.github.mjakubowski84.parquet4s.akka.indefinite

import akka.actor.{ActorSystem, CoordinatedShutdown}

import scala.concurrent.ExecutionContext

trait Akka {

  this: Logger =>

  implicit lazy val system: ActorSystem           = ActorSystem()
  implicit def executionContext: ExecutionContext = system.dispatcher
  val coordinatedShutdown: CoordinatedShutdown    = CoordinatedShutdown(system)

}
