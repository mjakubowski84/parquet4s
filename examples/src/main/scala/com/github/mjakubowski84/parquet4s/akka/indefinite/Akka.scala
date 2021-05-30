package com.github.mjakubowski84.parquet4s.akka.indefinite

import akka.actor.ActorSystem

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

trait Akka {

  this: Logger =>

  implicit lazy val system: ActorSystem           = ActorSystem()
  implicit def executionContext: ExecutionContext = system.dispatcher

  def stopAkka(): Unit = {
    logger.info("Stopping Akka...")
    Await.ready(system.terminate(), 1.second)
    ()
  }
}
