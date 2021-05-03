package com.github.mjakubowski84.parquet4s

import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.github.mjakubowski84.parquet4s.Cursor.DotPath
import com.github.mjakubowski84.parquet4s.ParquetPartitioningFlow._
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration

object ParquetPartitioningFlow {

  val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)
  private val TimerKey = "ParquetPartitioningFlow.Rotation"

  def builder[T](basePath: Path): Builder[T, T] = BuilderImpl(
    basePath = basePath,
    maxCount = DefaultMaxCount,
    maxDuration = DefaultMaxDuration,
    preWriteTransformation = identity,
    partitionBy = Seq.empty,
    writeOptions = ParquetWriter.Options(),
    postWriteHandler = None
  )

  /**
   * Builds an instance [[ParquetPartitioningFlow]]
   * @tparam T Type of message that flow accepts
   * @tparam W Schema of Parquet file that flow writes
   */
  trait Builder[T, W] {
    /**
     * @param maxCount max number of records to be written before file rotation
     */
    def withMaxCount(maxCount: Long): Builder[T, W]
    /**
     * @param maxDuration max time after which partition file is rotated
     */
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W]
    /**
     * @param writeOptions writer options used by the flow
     */
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W]
    /**
     * Sets partition paths that flow partitions data by. Can be empty.
     * Partition path can be a simple string column (e.g. "color") or a dot-separated path pointing nested string field
     * (e.g. "user.address.postcode"). Partition path is used to extract data from the entity and to create
     * a tree of subdirectories for partitioned files. Using aforementioned partitions effects in creation
     * of (example) following tree:
     * {{{
     * ../color=blue
     *      /user.address.postcode=XY1234/
     *      /user.address.postcode=AB4321/
     *   /color=green
     *      /user.address.postcode=XY1234/
     *      /user.address.postcode=CV3344/
     *      /user.address.postcode=GH6732/
     * }}}
     * Take <b>note</b>:
     * <ol>
     *   <li>PartitionBy must point a string field.</li>
     *   <li>Partitioning removes partition fields from the schema. Data is stored in name of subdirectory
     *       instead of Parquet file.</li>
     *   <li>Partitioning cannot end in having empty schema. If you remove all fields of the message you will
     *       get an error.</li>
     *   <li>Partitioned directories can be filtered effectively during reading.</li>
     * </ol>
     * @param partitionBy partition paths
     */
    def withPartitionBy(partitionBy: String*): Builder[T, W]
    /**
     * @param transformation function that is called by flow in order to obtain Parquet schema. Identity by default.
     * @tparam X Schema type
     */
    def withPreWriteTransformation[X](transformation: T => X): Builder[T, X]

    /**
     * Adds a handler after record writes, exposing some of the internal state of the flow.
     * Intended for lower level monitoring and control.
     *
     * @param handler a function called after writing a record,
     *                receiving a snapshot of the internal state of the flow as a parameter.
     */
    def withPostWriteHandler(handler: PostWriteState[T] => Unit): Builder[T, W]

    /**
     * Builds final flow
     */
    def build()(implicit
                schemaResolver: SkippingParquetSchemaResolver[W],
                encoder: ParquetRecordEncoder[W]): GraphStage[FlowShape[T, T]]
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

    def withMaxCount(maxCount: Long): Builder[T, W] = copy(maxCount = maxCount)
    def withMaxDuration(maxDuration: FiniteDuration): Builder[T, W] = copy(maxDuration = maxDuration)
    def withWriteOptions(writeOptions: ParquetWriter.Options): Builder[T, W] = copy(writeOptions = writeOptions)
    def withPartitionBy(partitionBy: String*): Builder[T, W] = copy(partitionBy = partitionBy)
    def build()(implicit
                schemaResolver: SkippingParquetSchemaResolver[W],
                encoder: ParquetRecordEncoder[W]
    ): GraphStage[FlowShape[T, T]] = {
      val partitionPaths = partitionBy.map(DotPath.apply)
      val schema = SkippingParquetSchemaResolver.resolveSchema[W](toSkip = partitionBy)
      val encode = (obj: W, vcc: ValueCodecConfiguration) => ParquetRecordEncoder.encode(obj, vcc)
      new ParquetPartitioningFlow[T, W](basePath, maxCount, maxDuration, preWriteTransformation,
        partitionPaths, encode, schema, writeOptions, postWriteHandler)
    }

    override def withPreWriteTransformation[X](transformation: T => X): Builder[T, X] =
      BuilderImpl(
        basePath = basePath,
        maxCount = maxCount,
        maxDuration = maxDuration,
        preWriteTransformation = transformation,
        partitionBy = partitionBy,
        postWriteHandler = postWriteHandler,
        writeOptions = writeOptions
      )

    override def withPostWriteHandler(handler: PostWriteState[T] => Unit): Builder[T, W] = copy(postWriteHandler = Some(handler))
  }

  case class PostWriteState[T](count: Long,
                               lastProcessed: T,
                               partitions: Set[Path],
                               flush: () => Unit
                              )

}

private class ParquetPartitioningFlow[T, W](
                                             basePath: Path,
                                             maxCount: Long,
                                             maxDuration: FiniteDuration,
                                             preWriteTransformation: T => W,
                                             partitionBy: Seq[DotPath],
                                             encode: (W, ValueCodecConfiguration) => RowParquetRecord,
                                             schema: MessageType,
                                             writeOptions: ParquetWriter.Options,
                                             postWriteHandler: Option[PostWriteState[T] => Unit]
                                           ) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T] = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger = LoggerFactory.getLogger("ParquetPartitioningFlow")
  private val vcc = writeOptions.toValueCodecConfiguration

  private class Logic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    private var writers: scala.collection.immutable.Map[Path, ParquetWriter.InternalWriter] = Map.empty
    private var count = 0L

    setHandlers(in, out, this)

    private def compressionExtension: String = writeOptions.compressionCodecName.getExtension
    private def newFileName: String = UUID.randomUUID().toString + compressionExtension + ".parquet"

    private def partition(record: RowParquetRecord): Path =
      partitionBy.foldLeft(basePath) {
        (path, partitionPath) =>
          val partitionName = DotPath.toString(partitionPath)
          record.remove(partitionPath) match {
            case Some(BinaryValue(binary)) => new Path(path, s"$partitionName=${binary.toStringUsingUTF8}")
            case None => throw new IllegalArgumentException(s"Field '$partitionName' does not exist.")
            case Some(NullValue) => throw new IllegalArgumentException(s"Field '$partitionName' is null.")
            case _ => throw new IllegalArgumentException(s"Non-string field '$partitionName' used for partitioning.")
          }
      }

    private def write(msg: T): Unit = {
      val valueToWrite = preWriteTransformation(msg)
      val record = encode(valueToWrite, vcc)
      val writerPath = partition(record)
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
      writer.write(record)
    }

    private def close(): Unit = {
      writers.valuesIterator.foreach(_.close())
      writers = Map.empty
      count = 0
    }

    override def preStart(): Unit =
      scheduleWithFixedDelay(TimerKey, maxDuration, maxDuration)

    override def onTimer(timerKey: Any): Unit =
      if (TimerKey == timerKey) {
        close()
      }

    override def onPush(): Unit = {
      val msg = grab(in)
      write(msg)
      count += 1

      postWriteHandler.foreach(_.apply(PostWriteState(count,
        lastProcessed = msg,
        partitions = writers.keySet,
        flush = () => close()
      )))

      if (count >= maxCount) {
        close()
      }

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
