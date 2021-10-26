package com.github.mjakubowski84.parquet4s

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.github.mjakubowski84.parquet4s.ParquetPartitioningFlow._
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

object ParquetPartitioningFlow {

  val DefaultMaxCount: Long              = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)

  def builder[T](basePath: Path): Builder[T, T] = BuilderImpl(
    basePath               = basePath,
    maxCount               = DefaultMaxCount,
    maxDuration            = DefaultMaxDuration,
    preWriteTransformation = identity,
    partitionBy            = Seq.empty,
    writeOptions           = ParquetWriter.Options(),
    postWriteHandler       = None
  )

  /** Builds an instance [[ParquetPartitioningFlow]]
    * @tparam T
    *   Type of message that flow accepts
    * @tparam W
    *   Schema of Parquet file that flow writes
    */
  trait Builder[T, W] {

    /** @param maxCount
      *   max number of records to be written before file rotation
      */
    def withMaxCount(maxCount: Long): Builder[T, W]

    /** @param maxDuration
      *   max time after which partition file is rotated
      */
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W]

    /** @param writeOptions
      *   writer options used by the flow
      */
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W]

    /** Sets partition paths that flow partitions data by. Can be empty. Partition path can be a simple string column
      * (e.g. "color") or a dot-separated path pointing nested string field (e.g. "user.address.postcode"). Partition
      * path is used to extract data from the entity and to create a tree of subdirectories for partitioned files. Using
      * aforementioned partitions effects in creation of (example) following tree:
      * {{{
      * ../color=blue
      *       /user.address.postcode=XY1234/
      *       /user.address.postcode=AB4321/
      *   /color=green
      *       /user.address.postcode=XY1234/
      *       /user.address.postcode=CV3344/
      *       /user.address.postcode=GH6732/
      * }}}
      * Take <b>note</b>: <ol> <li>PartitionBy must point a string field.</li> <li>Partitioning removes partition fields
      * from the schema. Data is stored in name of subdirectory instead of Parquet file.</li> <li>Partitioning cannot
      * end in having empty schema. If you remove all fields of the message you will get an error.</li> <li>Partitioned
      * directories can be filtered effectively during reading.</li> </ol>
      * @param partitionBy
      *   partition paths
      */
    def withPartitionBy(partitionBy: String*): Builder[T, W]

    /** @param transformation
      *   function that is called by flow in order to obtain Parquet schema. Identity by default.
      * @tparam X
      *   Schema type
      */
    def withPreWriteTransformation[X](transformation: T => X): Builder[T, X]

    /** Adds a handler after record writes, exposing some of the internal state of the flow. Intended for lower level
      * monitoring and control.
      *
      * @param handler
      *   a function called after writing a record, receiving a snapshot of the internal state of the flow as a
      *   parameter.
      */
    def withPostWriteHandler(handler: PostWriteState[T] => Unit): Builder[T, W]

    /** Builds final flow
      */
    def build()(implicit
        partitionLens: PartitionLens[W],
        schemaResolver: SkippingParquetSchemaResolver[W],
        encoder: SkippingParquetRecordEncoder[W]
    ): GraphStage[FlowShape[T, T]]
  }

  private case class BuilderImpl[T, W](
      basePath: Path,
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: T => W,
      partitionBy: Seq[String],
      writeOptions: ParquetWriter.Options,
      postWriteHandler: Option[PostWriteState[T] => Unit]
  ) extends Builder[T, W] {

    def withMaxCount(maxCount: Long): Builder[T, W]                          = copy(maxCount = maxCount)
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W]          = copy(maxDuration = maxDuration)
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W] = copy(writeOptions = writeOptions)
    def withPartitionBy(partitionBy: String*): Builder[T, W]                 = copy(partitionBy = partitionBy)
    def build()(implicit
        partitionLens: PartitionLens[W],
        schemaResolver: SkippingParquetSchemaResolver[W],
        encoder: SkippingParquetRecordEncoder[W]
    ): GraphStage[FlowShape[T, T]] = {
      val lenses = partitionBy.map(lensPath => (obj: W) => PartitionLens(obj, lensPath))
      val schema = SkippingParquetSchemaResolver.resolveSchema[W](toSkip = partitionBy)
      val encode = (obj: W, vcc: ValueCodecConfiguration) => SkippingParquetRecordEncoder.encode(partitionBy, obj, vcc)
      new ParquetPartitioningFlow[T, W](
        basePath,
        maxCount,
        maxDuration,
        preWriteTransformation,
        lenses,
        encode,
        schema,
        writeOptions,
        postWriteHandler
      )
    }

    override def withPreWriteTransformation[X](transformation: T => X): Builder[T, X] =
      BuilderImpl(
        basePath               = basePath,
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        preWriteTransformation = transformation,
        partitionBy            = partitionBy,
        postWriteHandler       = postWriteHandler,
        writeOptions           = writeOptions
      )

    override def withPostWriteHandler(handler: PostWriteState[T] => Unit): Builder[T, W] =
      copy(postWriteHandler = Some(handler))
  }

  case class PostWriteState[T](count: Long, lastProcessed: T, partitions: Set[Path], flush: () => Unit)

}

private class ParquetPartitioningFlow[T, W](
    basePath: Path,
    maxCount: Long,
    maxDuration: FiniteDuration,
    preWriteTransformation: T => W,
    partitionBy: Seq[W => (String, String)],
    encode: (W, ValueCodecConfiguration) => RowParquetRecord,
    schema: MessageType,
    writeOptions: ParquetWriter.Options,
    postWriteHandler: Option[PostWriteState[T] => Unit]
) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T]           = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T]         = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger         = LoggerFactory.getLogger("ParquetPartitioningFlow")
  private val vcc            = writeOptions.toValueCodecConfiguration

  private class Logic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    final case class WriterState(
        writer: ParquetWriter.InternalWriter,
        var written: Long
    )

    private var writers: Map[Path, WriterState] = Map.empty

    setHandlers(in, out, this)

    private def partitionPath(obj: W): Path = partitionBy.foldLeft(basePath) { case (path, partitionBy) =>
      val (key, value) = partitionBy(obj)
      new Path(path, s"$key=$value")
    }

    private def compressionExtension: String =
      writeOptions.compressionCodecName.getExtension

    private def newFileName: String =
      UUID.randomUUID().toString + compressionExtension + ".parquet"

    private def scheduleNextRotation(path: Path, delay: FiniteDuration): Unit =
      scheduleOnce(timerKey = path, delay = delay)

    private def write(msg: T): Unit = {
      val valueToWrite = preWriteTransformation(msg)
      val writerPath   = partitionPath(valueToWrite)
      val state = writers.get(writerPath) match {
        case Some(state) =>
          state

        case None =>
          logger.debug("Creating writer to write to [{}]", writerPath)

          val writer = ParquetWriter.internalWriter(
            path    = new Path(writerPath, newFileName),
            schema  = schema,
            options = writeOptions
          )

          val state = WriterState(
            writer  = writer,
            written = 0L
          )

          writers += writerPath -> state
          scheduleNextRotation(writerPath, maxDuration)
          state
      }

      state.writer.write(encode(valueToWrite, vcc))
      state.written += 1

      if (state.written >= maxCount) {
        close(writerPath, state)
      }
    }

    private def close(path: Path, state: WriterState): Unit = {
      cancelTimer(path)
      state.writer.close()
      writers -= path
    }

    private def close(): Unit = {
      writers.foreach { case (path, state) =>
        cancelTimer(path)
        state.writer.close()
      }

      writers = Map.empty
    }

    override def onTimer(timerKey: Any): Unit =
      writers.find(_._1 == timerKey) match {
        case Some((path, state)) =>
          close(path, state)

        case None =>
          logger.debug("Timer with key [{}] triggered but no state was found", timerKey)
      }

    override def onPush(): Unit = {
      val msg = grab(in)
      write(msg)

      postWriteHandler.foreach(
        _.apply(
          PostWriteState(
            count         = writers.values.map(_.written).sum,
            lastProcessed = msg,
            partitions    = writers.keySet,
            flush         = () => close()
          )
        )
      )

      push(out, msg)
    }

    override def onPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def postStop(): Unit = {
      close()
      super.postStop()
    }

  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new Logic()
}
