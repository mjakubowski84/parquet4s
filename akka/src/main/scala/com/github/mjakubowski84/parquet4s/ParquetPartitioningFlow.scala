package com.github.mjakubowski84.parquet4s

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory
import ParquetPartitioningFlow._

import scala.concurrent.duration.FiniteDuration

object ParquetPartitioningFlow {

  val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)
  private val TimerKey = "ParquetPartitioningFlow.Rotation"

  def builder[T](basePath: Path): Builder[T, T] = BuilderImpl(basePath)

  trait Builder[T, W] {
    def withMaxCount(maxCount: Long): Builder[T, W]
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W]
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W]
    def withPartitionBy(partitionBy: String*): Builder[T, W]
    def withPreWriteTransformation[X](transformation: T => X): Builder[T, X]
    def build()(implicit
                partitionLens: PartitionLens[W],
                schemaResolver: SkippingParquetSchemaResolver[W],
                encoder: SkippingParquetRecordEncoder[W]): GraphStage[FlowShape[T, T]]
  }

  private case class BuilderImpl[T, W](
                                        basePath: Path,
                                        maxCount: Long = DefaultMaxCount,
                                        maxDuration: FiniteDuration = DefaultMaxDuration,
                                        preWriteTransformation: T => W = identity[T] _,
                                        partitionBy: Seq[String]  = Seq.empty,
                                        writeOptions: ParquetWriter.Options = ParquetWriter.Options()
                                      ) extends Builder[T, W] {

    def withMaxCount(maxCount: Long): Builder[T, W] = copy(maxCount = maxCount)
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W] = copy(maxDuration = maxDuration)
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W] = copy(writeOptions = writeOptions)
    def withPartitionBy(partitionBy: String*): Builder[T, W] = copy(partitionBy = partitionBy)
    def build()(implicit
                partitionLens: PartitionLens[W],
                schemaResolver: SkippingParquetSchemaResolver[W],
                encoder: SkippingParquetRecordEncoder[W]
    ): GraphStage[FlowShape[T, T]] = {
      val lenses = partitionBy.map(lensPath => (obj: W) => PartitionLens(obj, lensPath) )
      val schema = SkippingParquetSchemaResolver.resolveSchema[W](toSkip = partitionBy)
      val encode = (obj: W, vcc: ValueCodecConfiguration) => SkippingParquetRecordEncoder.encode(partitionBy, obj, vcc)
      new ParquetPartitioningFlow[T, W](basePath, maxCount, maxDuration, preWriteTransformation, lenses, encode, schema, writeOptions)
    }

    override def withPreWriteTransformation[X](transformation: T => X): Builder[T, X] =
      BuilderImpl(
        basePath = basePath,
        maxCount = maxCount,
        maxDuration = maxDuration,
        preWriteTransformation = transformation,
        partitionBy = partitionBy,
        writeOptions = writeOptions
      )
  }

}

private class ParquetPartitioningFlow[T, W](
                                             basePath: Path,
                                             maxCount: Long,
                                             maxDuration: FiniteDuration,
                                             preWriteTransformation: T => W,
                                             partitionBy: Seq[W => (String, String)],
                                             encode: (W, ValueCodecConfiguration) => RowParquetRecord,
                                             schema: MessageType,
                                             writeOptions: ParquetWriter.Options
                                           ) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T] = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger = LoggerFactory.getLogger("ParquetPartitioningFlow")
  private val vcc = writeOptions.toValueCodecConfiguration

  private class Logic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    private var writers: scala.collection.immutable.Map[Path, ParquetWriter.InternalWriter] = Map.empty
    private var shouldRotate = true
    private var count = 0L

    setHandlers(in, out, this)

    private def partitionPath(obj: W): Path = partitionBy.foldLeft(basePath) {
      case (path, partitionBy) =>
        val (key, value) = partitionBy(obj)
        new Path(path, s"$key=$value")
    }

    private def compressionExtension = writeOptions.compressionCodecName.getExtension
    private def newFileName: String = UUID.randomUUID().toString + compressionExtension + ".parquet"

    private def write(msg: T): Unit = {
      val valueToWrite = preWriteTransformation(msg)
      val writerPath = partitionPath(valueToWrite)
      val writer = writers.get(writerPath) match {
        case Some(writer) =>
          writer
        case None =>
          if (logger.isDebugEnabled()) logger.debug(s"Creating writer to write to: " + writerPath)
          val writer = ParquetWriter.internalWriter(
            path = new Path(writerPath, newFileName),
            schema = schema,
            options = writeOptions
          )
          writers = writers.updated(writerPath, writer)
          writer
      }
      writer.write(encode(valueToWrite, vcc))
    }

    private def close(): Unit = {
      writers.valuesIterator.foreach(_.close())
      writers = Map.empty
    }

    override def preStart(): Unit =
      schedulePeriodically(TimerKey, maxDuration)

    override def onTimer(timerKey: Any): Unit =
      if (TimerKey == timerKey) {
        shouldRotate = true
      }

    override def onPush(): Unit = {
      if (shouldRotate) {
        close()
        shouldRotate = false
        count = 0
      }
      val msg = grab(in)
      write(msg)
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
      close()
      completeStage()
    }

  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new Logic()
}
