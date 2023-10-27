package com.github.mjakubowski84.parquet4s

object ScalaCompat {

  type NotUsed = org.apache.pekko.NotUsed
  def NotUsed = org.apache.pekko.NotUsed

  type Done = org.apache.pekko.Done
  def Done = org.apache.pekko.Done

  object stream {
    type Attributes = org.apache.pekko.stream.Attributes

    def ActorAttributes = org.apache.pekko.stream.ActorAttributes

    type Inlet[T] = org.apache.pekko.stream.Inlet[T]
    def Inlet = org.apache.pekko.stream.Inlet

    type Outlet[T] = org.apache.pekko.stream.Outlet[T]
    def Outlet = org.apache.pekko.stream.Outlet

    type Shape = org.apache.pekko.stream.Shape

    type FlowShape[I, O] = org.apache.pekko.stream.FlowShape[I, O]
    def FlowShape = org.apache.pekko.stream.FlowShape

    object stage {
      type GraphStage[S <: Shape] = org.apache.pekko.stream.stage.GraphStage[S]

      type GraphStageLogic = org.apache.pekko.stream.stage.GraphStageLogic

      type TimerGraphStageLogic = org.apache.pekko.stream.stage.TimerGraphStageLogic

      type InHandler = org.apache.pekko.stream.stage.InHandler

      type OutHandler = org.apache.pekko.stream.stage.OutHandler
    }

    object scaladsl {
      type Source[Out, Mat] = org.apache.pekko.stream.scaladsl.Source[Out, Mat]
      def Source = org.apache.pekko.stream.scaladsl.Source

      type Flow[In, Out, Mat] = org.apache.pekko.stream.scaladsl.Flow[In, Out, Mat]
      def Flow[T] = org.apache.pekko.stream.scaladsl.Flow[T]

      def Keep = org.apache.pekko.stream.scaladsl.Keep

      type Sink[In, Mat] = org.apache.pekko.stream.scaladsl.Sink[In, Mat]
      def Sink = org.apache.pekko.stream.scaladsl.Sink
    }
  }

  object pattern {
    type AskSupport = org.apache.pekko.pattern.AskSupport
  }

  object actor {
    type Actor = org.apache.pekko.actor.Actor
    def Actor = org.apache.pekko.actor.Actor

    type ActorRef = org.apache.pekko.actor.ActorRef
    def ActorRef = org.apache.pekko.actor.ActorRef

    type CoordinatedShutdown = org.apache.pekko.actor.CoordinatedShutdown
    def CoordinatedShutdown = org.apache.pekko.actor.CoordinatedShutdown

    type Cancellable = org.apache.pekko.actor.Cancellable

    type Props = org.apache.pekko.actor.Props
    def Props = org.apache.pekko.actor.Props

    type Scheduler = org.apache.pekko.actor.Scheduler

    type ActorSystem = org.apache.pekko.actor.ActorSystem
    def ActorSystem = org.apache.pekko.actor.ActorSystem
  }

  object util {
    type Timeout = org.apache.pekko.util.Timeout
    def Timeout = org.apache.pekko.util.Timeout
  }
}
