package com.github.mjakubowski84.parquet4s


import java.nio.file.Paths
import java.util.UUID

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

object ParquetPartitioningFlow {
  def apply[T: ParquetRecordEncoder : ParquetSchemaResolver](
                                                              basePath: String,
                                                              maxCount: Long,
                                                              maxDuration: FiniteDuration,
                                                              writeOptions: ParquetWriter.Options = ParquetWriter.Options()
                                                            ): GraphStage[FlowShape[T, T]] =
    new ParquetPartitioningFlow[T](basePath, maxCount, maxDuration, writeOptions)
}


private class ParquetPartitioningFlow[T: ParquetRecordEncoder : ParquetSchemaResolver](
                                                                          basePath: String,
                                                                            maxCount: Long,
                                                                            maxDuration: FiniteDuration,
                                                                            writeOptions: ParquetWriter.Options
                               ) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T] = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger = LoggerFactory.getLogger(basePath)

  private class FlowWithPassthroughLogic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    private var rotationCount = -1L
    private var writer: ParquetWriter[T] = _
    private val timerKey = "ParquetPartitioningFlow.rotation"
    private var shouldRotate = true
    private var count = 0L
    private var path = newPath

    setHandlers(in, out, this)

    def newPath: String = {
      // TODO add compression codec to path name
      Paths.get(basePath, UUID.randomUUID().toString + ".parquet").toString
    }

    override def preStart(): Unit = {
      schedulePeriodically(timerKey, maxDuration)
    }

    override def onTimer(timerKey: Any): Unit = {
      if (this.timerKey == timerKey) {
        shouldRotate = true
      }
    }

    override def onPush(): Unit = {
      if (shouldRotate) {
        if (writer != null) writer.close()
        rotationCount += 1
        path = newPath
        writer = ParquetWriter.writer[T](path, writeOptions)
        shouldRotate = false
        count = 0
      }

      val msg = grab(in)
      writer.write(msg)
      count += 1

      if (count >= maxCount) {
        shouldRotate = true
      }

      push(out, msg)
    }

    override def onPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def onUpstreamFinish(): Unit = {
      if (writer != null) writer.close()
      completeStage()
    }

  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new FlowWithPassthroughLogic()
}