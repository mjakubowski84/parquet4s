package com.github.mjakubowski84.parquet4s

object ScalaCompat {

  type NotUsed = akka.NotUsed
  def NotUsed = akka.NotUsed

  type Done = akka.Done
  def Done = akka.Done

  object stream {
    type Attributes = akka.stream.Attributes

    def ActorAttributes = akka.stream.ActorAttributes

    type Inlet[T] = akka.stream.Inlet[T]
    def Inlet = akka.stream.Inlet

    type Outlet[T] = akka.stream.Outlet[T]
    def Outlet = akka.stream.Outlet

    type Shape = akka.stream.Shape

    type FlowShape[I, O] = akka.stream.FlowShape[I, O]
    def FlowShape = akka.stream.FlowShape

    object stage {
      type GraphStage[S <: Shape] = akka.stream.stage.GraphStage[S]

      type GraphStageLogic = akka.stream.stage.GraphStageLogic

      type TimerGraphStageLogic = akka.stream.stage.TimerGraphStageLogic

      type InHandler = akka.stream.stage.InHandler

      type OutHandler = akka.stream.stage.OutHandler
    }

    object scaladsl {
      type Source[Out, Mat] = akka.stream.scaladsl.Source[Out, Mat]
      def Source = akka.stream.scaladsl.Source

      type Flow[In, Out, Mat] = akka.stream.scaladsl.Flow[In, Out, Mat]
      def Flow[T] = akka.stream.scaladsl.Flow[T]

      def Keep = akka.stream.scaladsl.Keep

      type Sink[In, Mat] = akka.stream.scaladsl.Sink[In, Mat]
      def Sink = akka.stream.scaladsl.Sink
    }
  }

  object pattern {
    type AskSupport = akka.pattern.AskSupport
  }

  object actor {
    type Actor = akka.actor.Actor
    def Actor = akka.actor.Actor

    type ActorRef = akka.actor.ActorRef
    def ActorRef = akka.actor.ActorRef

    type CoordinatedShutdown = akka.actor.CoordinatedShutdown
    def CoordinatedShutdown = akka.actor.CoordinatedShutdown

    type Cancellable = akka.actor.Cancellable

    type Props = akka.actor.Props
    def Props = akka.actor.Props

    type Scheduler = akka.actor.Scheduler

    type ActorSystem = akka.actor.ActorSystem
    def ActorSystem = akka.actor.ActorSystem
  }

  object util {
    type Timeout = akka.util.Timeout
    def Timeout = akka.util.Timeout
  }
}
