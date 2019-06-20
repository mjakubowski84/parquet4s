package com.github.mjakubowski84.parquet4s

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

trait Akka {

  this: Logger =>

  implicit lazy val system: ActorSystem = ActorSystem()
  implicit lazy val materializer: Materializer = ActorMaterializer()
  implicit def executionContext: ExecutionContext = system.dispatcher

  def stopAkka(): Unit = {
    logger.info("Stopping Akka...")
    Await.ready(system.terminate(), 1.second)
  }
}
