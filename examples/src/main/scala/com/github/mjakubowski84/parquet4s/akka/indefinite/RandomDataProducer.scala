package com.github.mjakubowski84.parquet4s.akka.indefinite

import akka.actor.{Actor, ActorRef, Cancellable, Props, Scheduler}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

object RandomDataProducer {

  private val words = Seq("Example", "how", "to", "setup", "indefinite", "stream", "with", "Parquet", "writer")

}

trait RandomDataProducer {

  this: Akka with Logger with Kafka =>

  import RandomDataProducer._

  private def nextWord: String = words(Random.nextInt(words.size - 1))
  private def action(): Unit = sendKafkaMessage(nextWord)

  private lazy val scheduler: ActorRef = system.actorOf(FluctuatingSchedulerActor.props(action))
  implicit private val stopTimeout: Timeout = new Timeout(FluctuatingSchedulerActor.MaxDelay)

  def startDataProducer(): Unit = {
    logger.info("Starting scheduler that sends messages to Kafka...")
    scheduler ! FluctuatingSchedulerActor.Start
  }

  def stopDataProducer(): Unit = {
    logger.info("Stopping scheduler...")
    Await.ready(scheduler.ask(FluctuatingSchedulerActor.Stop), Duration.Inf)
    ()
  }

}

private object FluctuatingSchedulerActor {

  case object Start
  case object ScheduleNext
  case object Stop

  val MinDelay: FiniteDuration = 1.milli
  val MaxDelay: FiniteDuration = 500.millis
  val StartDelay: FiniteDuration = 100.millis

  trait Direction
  case object Up extends Direction
  case object Down extends Direction

  def props(action: () => Unit): Props = Props(new FluctuatingSchedulerActor(action))

}

private class FluctuatingSchedulerActor(action: () => Unit) extends Actor {

  import FluctuatingSchedulerActor._

  implicit def executionContext: ExecutionContext = context.system.dispatcher
  def scheduler: Scheduler = context.system.scheduler
  var scheduled: Option[Cancellable] = None

  override def receive: Receive = {
    case Start =>
      self ! ScheduleNext
      context.become(scheduling(StartDelay, direction = Down), discardOld = true)
  }

  def scheduling(delay: FiniteDuration, direction: Direction): Receive = {
    case ScheduleNext =>
      action()

      val rate = Random.nextFloat() / 10.0f
      val step = (delay.toMillis * rate).millis
      val (newDirection, newDelay) = direction match {
        case Up if delay + step < MaxDelay =>
          (Up, delay + step)
        case Up =>
          (Down, delay - step)
        case Down if delay - step > MinDelay =>
          (Down, delay - step)
        case Down =>
          (Up, delay + step)
      }

      scheduled = Some(scheduler.scheduleOnce(delay, self, ScheduleNext))
      context.become(scheduling(newDelay, newDirection), discardOld = true)

    case Stop =>
      scheduled.foreach(_.cancel())
      context.stop(self)
  }

}
