package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.*
import cats.effect.std.Queue
import cats.effect.syntax.all.*
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.*
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import fs2.{Pipe, Pull, Stream}
import org.apache.parquet.hadoop.ParquetWriter as HadoopParquetWriter
import org.apache.parquet.schema.MessageType

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object rotatingWriter {

  val DefaultMaxCount: Long              = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)

  trait ViaParquet[F[_]] {

    /** @tparam T
      *   schema type
      * @return
      *   Builder of pipe that processes data of specified schema
      */
    def of[T]: TypedBuilder[F, T, T]

    /** @return
      *   Builder of pipe that processes generic records
      */
    def generic: GenericBuilder[F]
  }

  private[parquet4s] class ViaParquetImpl[F[_]: Async] extends ViaParquet[F] {
    override def of[T]: TypedBuilder[F, T, T] = TypedBuilderImpl[F, T, T](
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = t => Stream.emit(t),
      partitionBy            = Seq.empty,
      postWriteHandlerOpt    = None,
      writeOptions           = ParquetWriter.Options()
    )
    override def generic: GenericBuilder[F] = GenericBuilderImpl(
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = Stream.emit,
      partitionBy            = Seq.empty,
      postWriteHandlerOpt    = None,
      writeOptions           = ParquetWriter.Options()
    )
  }

  trait Builder[F[_], T, W, Self] {

    /** @param maxCount
      *   max number of records to be written before file rotation
      */
    def maxCount(maxCount: Long): Self

    /** @param maxDuration
      *   max time after which partition file is rotated
      */
    def maxDuration(maxDuration: FiniteDuration): Self

    /** @param writeOptions
      *   writer options used by the flow
      */
    def options(writeOptions: ParquetWriter.Options): Self

    /** Sets partition paths that stream partitions data by. Can be empty. Partition path can be a simple string column
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
      * from the schema. Data is stored in the name of subdirectory instead of Parquet file.</li> <li>Partitioning
      * cannot end in having empty schema. If you remove all fields of the message you will get an error.</li>
      * <li>Partitioned directories can be filtered effectively during reading.</li> </ol>
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
      * @param postWriteHandler
      *   an effect called after writing a record, receiving a snapshot of the internal state of the flow as a
      *   parameter.
      */
    def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): Self

  }

  trait GenericBuilder[F[_]] extends Builder[F, RowParquetRecord, RowParquetRecord, GenericBuilder[F]] {

    /** @param transformation
      *   function that is called by stream in order to transform data to final write format. Identity by default.
      */
    def preWriteTransformation(transformation: RowParquetRecord => Stream[F, RowParquetRecord]): GenericBuilder[F]

    /** Builds final writer pipe.
      */
    def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord]
  }

  trait TypedBuilder[F[_], T, W] extends Builder[F, T, W, TypedBuilder[F, T, W]] {

    /** @param transformation
      *   function that is called by stream in order to transform data to final write format. Identity by default.
      * @tparam X
      *   Schema type
      */
    def preWriteTransformation[X](transformation: T => Stream[F, X]): TypedBuilder[F, T, X]

    /** Builds final writer pipe.
      */
    def write(basePath: Path)(implicit
        schemaResolver: ParquetSchemaResolver[W],
        encoder: ParquetRecordEncoder[W]
    ): Pipe[F, T, T]
  }

  private case class GenericBuilderImpl[F[_]: Async](
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: RowParquetRecord => Stream[F, RowParquetRecord],
      partitionBy: Seq[ColumnPath],
      postWriteHandlerOpt: Option[PostWriteHandler[F, RowParquetRecord]],
      writeOptions: ParquetWriter.Options
  ) extends GenericBuilder[F] {
    override def maxCount(maxCount: Long): GenericBuilder[F]                     = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): GenericBuilder[F]     = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): GenericBuilder[F] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): GenericBuilder[F]        = copy(partitionBy = partitionBy)
    override def preWriteTransformation(
        transformation: RowParquetRecord => Stream[F, RowParquetRecord]
    ): GenericBuilder[F] =
      copy(preWriteTransformation = transformation)
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, RowParquetRecord]): GenericBuilder[F] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord] =
      rotatingWriter.write[F, RowParquetRecord, RowParquetRecord](
        basePath,
        Async[F].pure(schema),
        maxCount,
        maxDuration,
        partitionBy,
        preWriteTransformation,
        postWriteHandlerOpt,
        writeOptions
      )
  }

  private case class TypedBuilderImpl[F[_]: Async, T, W](
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: T => Stream[F, W],
      partitionBy: Seq[ColumnPath],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      writeOptions: ParquetWriter.Options
  ) extends TypedBuilder[F, T, W] {

    override def maxCount(maxCount: Long): TypedBuilder[F, T, W]                     = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): TypedBuilder[F, T, W]     = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): TypedBuilder[F, T, W] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): TypedBuilder[F, T, W]        = copy(partitionBy = partitionBy)
    override def preWriteTransformation[X](transformation: T => Stream[F, X]): TypedBuilder[F, T, X] =
      TypedBuilderImpl(
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        preWriteTransformation = transformation,
        partitionBy            = partitionBy,
        writeOptions           = writeOptions,
        postWriteHandlerOpt    = postWriteHandlerOpt
      )
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): TypedBuilder[F, T, W] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(
        basePath: Path
    )(implicit schemaResolver: ParquetSchemaResolver[W], encoder: ParquetRecordEncoder[W]): Pipe[F, T, T] = {
      val schemaF = Sync[F].catchNonFatal(ParquetSchemaResolver.resolveSchema[W](partitionBy))
      rotatingWriter.write[F, T, W](
        basePath,
        schemaF,
        maxCount,
        maxDuration,
        partitionBy,
        preWriteTransformation,
        postWriteHandlerOpt,
        writeOptions
      )
    }
  }

  type PostWriteHandler[F[_], T] = PostWriteState[F, T] => F[Unit]

  /** Represent the state of writer after processing of `processedData`.
    * @param processedData
    *   Processed input element
    * @param modifiedPartitions
    *   State of partitions that has been written in effect of processing the element <i>T</i>. More than one partition
    *   can be modified due to <i>preWriteTransformation</i>. The map contains values representing total number of
    *   writes to a single file (number of writes to the partition after last rotation).
    * @param flush
    *   Flushes all writes to given partition and rotates the file.
    * @tparam F
    *   effect type
    * @tparam T
    *   type of input data
    */
  case class PostWriteState[F[_], T](processedData: T, modifiedPartitions: Map[Path, Long], flush: Path => F[Unit])

  sealed private trait WriterEvent[F[_], T, W]
  private case class DataEvent[F[_], T, W](data: Stream[F, W], out: T) extends WriterEvent[F, T, W]
  private case class RotateEvent[F[_], T, W](partition: Path) extends WriterEvent[F, T, W]
  private case class StopEvent[F[_], T, W]() extends WriterEvent[F, T, W]

  private object RecordWriter {

    private def newFileName(options: ParquetWriter.Options): String = {
      val compressionExtension = options.compressionCodecName.getExtension
      UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    def apply[F[_], T, W](
        basePath: Path,
        schema: MessageType,
        options: ParquetWriter.Options,
        eventQueue: Queue[F, WriterEvent[F, T, W]],
        maxDuration: FiniteDuration
    )(implicit F: Async[F]): F[RecordWriter[F]] =
      F.uncancelable { _ =>
        for {
          internalWrite <- F.blocking(
            ParquetWriter.internalWriter(basePath.append(newFileName(options)), schema, options)
          )
          rotationFiber <- F.delayBy(eventQueue.offer(RotateEvent[F, T, W](basePath)), maxDuration).start
        } yield new RecordWriter(internalWrite, rotationFiber)
      }
  }

  private class RecordWriter[F[_]: Sync](
      internalWriter: ParquetWriter.InternalWriter,
      rotationFiber: Fiber[F, Throwable, Unit]
  )(implicit F: Async[F]) {

    var count: Long = 0

    def write(record: RowParquetRecord): F[Long] = F.blocking {
      internalWriter.write(record)
      count = count + 1
      count
    }

    def dispose: F[Unit] =
      F.uncancelable { _ =>
        rotationFiber.cancel >> F.blocking(internalWriter.close())
      }
  }

  private class RotatingWriter[T, W, F[_]](
      basePath: Path,
      options: ParquetWriter.Options,
      maxCount: Long,
      maxDuration: FiniteDuration,
      partitionBy: List[ColumnPath],
      schema: MessageType,
      encode: W => F[RowParquetRecord],
      eventQueue: Queue[F, WriterEvent[F, T, W]],
      logger: Logger[F],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]]
  )(implicit F: Async[F]) {

    private val writers = TrieMap.empty[Path, RecordWriter[F]]

    private def writeEntitiesPull(
        entityStream: Stream[F, W],
        modifiedPartitions: Map[Path, Long]
    ): Pull[F, T, Map[Path, Long]] =
      entityStream.pull.uncons1.flatMap {
        case Some((entity, tail)) =>
          Pull.eval(write(entity)).flatMap { case (partition, count) =>
            writeEntitiesPull(tail, modifiedPartitions.updated(partition, count))
          }
        case None =>
          Pull.pure(modifiedPartitions)
      }

    private def getOrCreateWriter(basePath: Path): F[RecordWriter[F]] =
      writers.get(basePath) match {
        case Some(writer) =>
          F.pure(writer)
        case None =>
          F.uncancelable { _ =>
            for {
              writer            <- RecordWriter(basePath, schema, options, eventQueue, maxDuration)
              existingWriterOpt <- F.delay(writers.putIfAbsent(basePath, writer))
              resultingWriter <- existingWriterOpt match {
                case Some(existing) => writer.dispose.as(existing) // should not happen
                case None           => F.pure(writer)
              }
            } yield resultingWriter
          }
      }

    private def write(entity: W): F[(Path, Long)] =
      for {
        record <- encode(entity)
        partitioning <- partitionBy.foldLeft(F.pure(basePath -> record)) { case (f, currentPartition) =>
          f.flatMap { case (currentPath, currentRecord) =>
            partition(currentRecord, currentPartition).map { case (partitionPath, partitionValue, modifiedRecord) =>
              currentPath.append(s"$partitionPath=$partitionValue") -> modifiedRecord
            }
          }
        }
        (path, partitionedRecord) = partitioning
        writer <- getOrCreateWriter(path)
        count  <- writer.write(partitionedRecord)
        _ <-
          if (count >= maxCount) {
            F.uncancelable(_ => dispose(path))
          } else {
            F.unit
          }
      } yield path -> count

    private def partition(
        record: RowParquetRecord,
        partitionPath: ColumnPath
    ): F[(ColumnPath, String, RowParquetRecord)] =
      record.removed(partitionPath) match {
        case (None, _) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' does not exist."))
        case (Some(NullValue), _) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' is null."))
        case (Some(BinaryValue(binary)), modifiedRecord) =>
          F.catchNonFatal((partitionPath, binary.toStringUsingUTF8, modifiedRecord))
        case _ =>
          F.raiseError(new IllegalArgumentException(s"Non-string field '$partitionPath' used for partitioning."))
      }

    private def disposeAll: F[Unit] =
      F.uncancelable { _ =>
        Stream
          .suspend(Stream.iterable(writers.values))
          .evalMap(_.dispose)
          .onFinalize(F.delay(writers.clear()))
          .compile
          .drain
      }

    private def dispose(partition: Path): F[Unit] =
      F.uncancelable { _ =>
        writers.remove(partition).traverse_(_.dispose)
      }

    private def rotatePull(partitions: Iterable[Path]): Pull[F, T, Unit] =
      partitions
        .map(partition => Pull.eval(logger.debug(s"Rotating $partition")) >> Pull.eval(dispose(partition)))
        .reduceOption(_ >> _)
        .getOrElse(Pull.done)

    private def postWriteHandlerPull(
        out: T,
        partitionsState: Map[Path, Long]
    ): Pull[F, T, List[Path]] =
      postWriteHandlerOpt.fold(Pull.eval[F, List[Path]](F.pure(List.empty)))(handler =>
        Pull.eval {
          for {
            partitionsToFlushRef <- Ref.of[F, List[Path]](List.empty)
            state = PostWriteState[F, T](
              processedData      = out,
              modifiedPartitions = partitionsState,
              flush              = partition => partitionsToFlushRef.update(partitions => partition +: partitions)
            )
            _                 <- handler(state)
            partitionsToFlush <- partitionsToFlushRef.get
          } yield partitionsToFlush
        }
      )

    private def writeAllEventsPull(in: Stream[F, WriterEvent[F, T, W]]): Pull[F, T, Unit] =
      in.pull.uncons1.flatMap {
        case Some((DataEvent(dataStream, out), tail)) if postWriteHandlerOpt.isEmpty =>
          writeEntitiesPull(dataStream, Map.empty) >> Pull.output1(out) >> writeAllEventsPull(tail)
        case Some((DataEvent(dataStream, out), tail)) =>
          writeEntitiesPull(dataStream, Map.empty).flatMap { modifiedPartitions =>
            postWriteHandlerPull(out, modifiedPartitions).flatMap {
              case Nil                => Pull.output1(out) >> writeAllEventsPull(tail)
              case partitionsToRotate => rotatePull(partitionsToRotate) >> Pull.output1(out) >> writeAllEventsPull(tail)
            }
          }
        case Some((RotateEvent(partition), tail)) =>
          rotatePull(Iterable(partition)) >> writeAllEventsPull(tail)
        case _ =>
          Pull.done
      }

    def writeAllEvents(in: Stream[F, WriterEvent[F, T, W]]): Stream[F, T] =
      writeAllEventsPull(in).stream.onFinalize(disposeAll)
  }

  private def write[F[_], T, W: ParquetRecordEncoder](
      basePath: Path,
      schemaF: F[MessageType],
      maxCount: Long,
      maxDuration: FiniteDuration,
      partitionBy: Seq[ColumnPath],
      prewriteTransformation: T => Stream[F, W],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      options: ParquetWriter.Options
  )(implicit F: Async[F]): Pipe[F, T, T] =
    in =>
      for {
        schema                  <- Stream.eval(schemaF)
        valueCodecConfiguration <- Stream.eval(F.catchNonFatal(ValueCodecConfiguration(options)))
        encode = { (entity: W) => F.catchNonFatal(ParquetRecordEncoder.encode[W](entity, valueCodecConfiguration)) }
        logger     <- Stream.eval(logger[F](this.getClass))
        eventQueue <- Stream.eval(Queue.unbounded[F, WriterEvent[F, T, W]])
        rotatingWriter <- Stream.emit(
          new RotatingWriter[T, W, F](
            basePath            = basePath,
            options             = options,
            maxCount            = maxCount,
            maxDuration         = maxDuration,
            partitionBy         = partitionBy.toList,
            schema              = schema,
            encode              = encode,
            eventQueue          = eventQueue,
            logger              = logger,
            postWriteHandlerOpt = postWriteHandlerOpt
          )
        )
        eventStream = Stream(
          Stream.fromQueueUnterminated(eventQueue),
          in
            .map { inputElement =>
              DataEvent[F, T, W](prewriteTransformation(inputElement), inputElement)
            }
            .append(Stream.emit(StopEvent[F, T, W]()))
        ).parJoin(maxOpen = 2)
        out <- rotatingWriter.writeAllEvents(eventStream)
      } yield out

}
