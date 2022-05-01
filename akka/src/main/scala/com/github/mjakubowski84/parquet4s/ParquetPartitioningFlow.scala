package com.github.mjakubowski84.parquet4s

import akka.stream.stage.*
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.parquet.hadoop.ParquetWriter as HadoopParquetWriter
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import com.github.mjakubowski84.parquet4s.ParquetPartitioningFlow.*

import scala.collection.concurrent.TrieMap

object ParquetPartitioningFlow {

  val DefaultMaxCount: Long              = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)

  trait ViaParquet {

    /** @tparam T
      *   schema type
      * @return
      *   builder of flow that processes data of given schema
      */
    def of[T]: TypedBuilder[T, T]

    /** @return
      *   builder of flow that processes generic records
      */
    def generic: GenericBuilder
  }

  private[parquet4s] object ViaParquetImpl extends ViaParquet {
    override def of[T]: TypedBuilder[T, T] = TypedBuilderImpl(
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = t => Iterable(t),
      partitionBy            = Seq.empty,
      writeOptions           = ParquetWriter.Options(),
      postWriteHandler       = None
    )
    override def generic: GenericBuilder = GenericBuilderImpl(
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = record => Iterable(record),
      partitionBy            = Seq.empty,
      options                = ParquetWriter.Options(),
      postWriteHandler       = None
    )
  }

  /** Builds an instance of [[ParquetPartitioningFlow]]
    * @tparam T
    *   Type of message that flow accepts
    * @tparam W
    *   Schema of Parquet file that flow writes
    */
  trait Builder[T, W, Self] {

    /** @param maxCount
      *   max number of records to be written before file rotation
      */
    def maxCount(maxCount: Long): Self

    /** @param maxDuration
      *   max time after which partition file is rotated
      */
    def maxDuration(maxDuration: FiniteDuration): Self

    /** @param options
      *   writer options used by the flow
      */
    def options(options: ParquetWriter.Options): Self

    /** Sets partition paths that flow partitions data by. Can be empty. Partition path can be a simple string column
      * (e.g. "color") or a path pointing nested string field (e.g. "user.address.postcode"). Partition path is used to
      * extract data from the entity and to create a tree of subdirectories for partitioned files. Using aforementioned
      * partitions effects in creation of (example) following tree:
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
      *
      * @param partitionBy
      *   [[ColumnPath]]s to partition by
      */
    def partitionBy(partitionBy: ColumnPath*): Self

    /** Adds a handler after record writes, exposing some of the internal state of the flow. Intended for lower level
      * monitoring and control.
      *
      * Please note that the handler is invoked after each input element is processed and not after each write. It is so
      * because <i>postWriteHandler</i> may produce multiple records for a single input element.
      *
      * @param handler
      *   a function called after writing a record, receiving a snapshot of the internal state of the flow as a
      *   parameter.
      */
    def postWriteHandler(handler: PostWriteState[T] => Unit): Self

  }

  trait GenericBuilder extends Builder[RowParquetRecord, RowParquetRecord, GenericBuilder] {

    /** @param transformation
      *   function that is called by flow in order to transform record to final write format. Identity by default.
      */
    def preWriteTransformation(transformation: RowParquetRecord => Iterable[RowParquetRecord]): GenericBuilder

    /** Builds a final flow
      */
    def write(basePath: Path, schema: MessageType): GraphStage[FlowShape[RowParquetRecord, RowParquetRecord]]
  }

  trait TypedBuilder[T, W] extends Builder[T, W, TypedBuilder[T, W]] {

    /** @param transformation
      *   function that is called by flow in order to transform data to final write format. Identity by default.
      * @tparam X
      *   Schema type
      */
    def preWriteTransformation[X](transformation: T => Iterable[X]): TypedBuilder[T, X]

    /** Builds a final flow
      */
    def write(
        basePath: Path
    )(implicit schemaResolver: ParquetSchemaResolver[W], encoder: ParquetRecordEncoder[W]): GraphStage[FlowShape[T, T]]
  }

  private case class GenericBuilderImpl(
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: RowParquetRecord => Iterable[RowParquetRecord],
      partitionBy: Seq[ColumnPath],
      options: ParquetWriter.Options,
      postWriteHandler: Option[PostWriteState[RowParquetRecord] => Unit]
  ) extends GenericBuilder {

    override def maxCount(maxCount: Long): GenericBuilder                 = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): GenericBuilder = copy(maxDuration = maxDuration)
    override def options(options: ParquetWriter.Options): GenericBuilder  = copy(options = options)
    override def partitionBy(partitionBy: ColumnPath*): GenericBuilder    = copy(partitionBy = partitionBy)
    override def preWriteTransformation(
        transformation: RowParquetRecord => Iterable[RowParquetRecord]
    ): GenericBuilder =
      copy(preWriteTransformation = transformation)
    override def postWriteHandler(handler: PostWriteState[RowParquetRecord] => Unit): GenericBuilder =
      copy(postWriteHandler = Some(handler))

    override def write(
        basePath: Path,
        schema: MessageType
    ): GraphStage[FlowShape[RowParquetRecord, RowParquetRecord]] = {
      implicit val resolver: ParquetSchemaResolver[RowParquetRecord] =
        RowParquetRecord.genericParquetSchemaResolver(schema)
      val finalSchema = ParquetSchemaResolver.resolveSchema[RowParquetRecord](toSkip = partitionBy)
      val encode      = (record: RowParquetRecord, _: ValueCodecConfiguration) => record

      new ParquetPartitioningFlow[RowParquetRecord, RowParquetRecord](
        basePath,
        maxCount,
        maxDuration,
        preWriteTransformation,
        partitionBy,
        encode,
        finalSchema,
        options,
        postWriteHandler
      )
    }
  }

  private case class TypedBuilderImpl[T, W](
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: T => Iterable[W],
      partitionBy: Seq[ColumnPath],
      writeOptions: ParquetWriter.Options,
      postWriteHandler: Option[PostWriteState[T] => Unit]
  ) extends TypedBuilder[T, W] {

    override def maxCount(maxCount: Long): TypedBuilder[T, W]                 = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): TypedBuilder[T, W] = copy(maxDuration = maxDuration)
    override def options(options: ParquetWriter.Options): TypedBuilder[T, W]  = copy(writeOptions = options)
    override def partitionBy(partitionBy: ColumnPath*): TypedBuilder[T, W]    = copy(partitionBy = partitionBy)
    override def preWriteTransformation[X](transformation: T => Iterable[X]): TypedBuilder[T, X] =
      TypedBuilderImpl(
        maxCount,
        maxDuration,
        transformation,
        partitionBy,
        writeOptions,
        postWriteHandler
      )
    override def postWriteHandler(handler: PostWriteState[T] => Unit): TypedBuilder[T, W] =
      copy(postWriteHandler = Some(handler))

    override def write(basePath: Path)(implicit
        schemaResolver: ParquetSchemaResolver[W],
        encoder: ParquetRecordEncoder[W]
    ): GraphStage[FlowShape[T, T]] = {
      val schema = ParquetSchemaResolver.resolveSchema[W](toSkip = partitionBy)
      val encode = (obj: W, vcc: ValueCodecConfiguration) => ParquetRecordEncoder.encode(obj, vcc)
      new ParquetPartitioningFlow[T, W](
        basePath,
        maxCount,
        maxDuration,
        preWriteTransformation,
        partitionBy,
        encode,
        schema,
        writeOptions,
        postWriteHandler
      )
    }
  }

  /** Represent the state of writer after processing of `processedData`.
    * @param processedData
    *   Processed input element
    * @param modifiedPartitions
    *   State of partitions that has been written in effect of processing the element <i>T</i>. More than one partition
    *   can be modified due to <i>preWriteTransformation</i>. The map contains values representing total number of
    *   writes to a single file (number of writes to the partition after last rotation).
    * @param flush
    *   Flushes all writes to given partition and rotates the file.
    * @tparam T
    *   type of input data
    */
  case class PostWriteState[T](processedData: T, modifiedPartitions: Map[Path, Long], flush: Path => Unit)

}

private class ParquetPartitioningFlow[T, W](
    basePath: Path,
    maxCount: Long,
    maxDuration: FiniteDuration,
    preWriteTransformation: T => Iterable[W],
    partitionBy: Seq[ColumnPath],
    encode: (W, ValueCodecConfiguration) => RowParquetRecord,
    schema: MessageType,
    writeOptions: ParquetWriter.Options,
    postWriteHandler: Option[PostWriteState[T] => Unit]
) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T]           = Inlet[T]("ParquetPartitioningFlow.in")
  val out: Outlet[T]         = Outlet[T]("ParquetPartitioningFlow.out")
  val shape: FlowShape[T, T] = FlowShape.of(in, out)
  private val logger         = LoggerFactory.getLogger("ParquetPartitioningFlow")
  private val vcc            = ValueCodecConfiguration(writeOptions)

  private class Logic extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
    case class WriterState(
        writer: ParquetWriter.InternalWriter,
        var written: Long
    )

    private val writers = TrieMap.empty[Path, WriterState]

    setHandlers(in, out, this)

    private val compressionExtension: String = writeOptions.compressionCodecName.getExtension
    private def newFileName: String          = UUID.randomUUID().toString + compressionExtension + ".parquet"

    private def partition(record: RowParquetRecord): (Path, RowParquetRecord) =
      partitionBy.foldLeft(basePath -> record) { case ((currentPath, currentRecord), partitionPath) =>
        currentRecord.removed(partitionPath) match {
          case (Some(BinaryValue(binary)), modifiedRecord) =>
            Path(currentPath, s"$partitionPath=${binary.toStringUsingUTF8}") -> modifiedRecord
          case (None, _) =>
            throw new IllegalArgumentException(s"Field '$partitionPath' does not exist.")
          case (Some(NullValue), _) =>
            throw new IllegalArgumentException(s"Field '$partitionPath' is null.")
          case _ =>
            throw new IllegalArgumentException(s"Non-string field '$partitionPath' used for partitioning.")
        }
      }

    private def scheduleNextRotation(path: Path, delay: FiniteDuration): Unit =
      scheduleOnce(timerKey = path, delay = delay)

    private def write(msg: T): Map[Path, Long] = {
      val valuesToWrite              = preWriteTransformation(msg)
      val records                    = valuesToWrite.map(value => encode(value, vcc))
      val pathsAndPartitionedRecords = records.map(partition)

      pathsAndPartitionedRecords.foldLeft(Map.empty[Path, Long]) {
        case (modifiedPartitions, (writerPath, partitionedRecord)) =>
          val state = writers.getOrElseUpdate(
            writerPath, {
              logger.debug("Creating writer to write to [{}]", writerPath)

              val writer = ParquetWriter.internalWriter(
                path    = Path(writerPath, newFileName),
                schema  = schema,
                options = writeOptions
              )

              val state = WriterState(
                writer  = writer,
                written = 0L
              )

              scheduleNextRotation(writerPath, maxDuration)
              state
            }
          )

          state.writer.write(partitionedRecord)
          state.written += 1

          modifiedPartitions.updated(writerPath, state.written)
      }
    }

    private def close(path: Path): Unit =
      writers.remove(path) match {
        case Some(writerState) =>
          cancelTimer(path)
          writerState.writer.close()
        case None =>
          logger.debug("Trying to close a writer for a path [{}], no state was found", path)
      }

    override def onTimer(timerKey: Any): Unit =
      timerKey match {
        case path: Path => close(path)
        case _          => // ignore
      }

    override def onPush(): Unit = {
      val msg                = grab(in)
      val modifiedPartitions = write(msg)

      postWriteHandler.foreach(
        _.apply(
          PostWriteState(
            processedData      = msg,
            modifiedPartitions = modifiedPartitions,
            flush              = partition => close(partition)
          )
        )
      )

      modifiedPartitions.foreach {
        case (path, count) if count >= maxCount =>
          close(path)
        case _ => // ignore
      }

      push(out, msg)
    }

    override def onPull(): Unit =
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }

    override def postStop(): Unit = {
      writers.keySet.foreach { path =>
        close(path)
      }

      super.postStop()
    }

  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new Logic()
}
