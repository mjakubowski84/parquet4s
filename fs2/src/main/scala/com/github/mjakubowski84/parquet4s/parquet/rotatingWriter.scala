package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.*
import cats.effect.syntax.all.*
import cats.implicits.*
import com.github.mjakubowski84.parquet4s.*
import com.github.mjakubowski84.parquet4s.compat.MapCompat
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import fs2.{Pipe, Pull, Stream}
import org.apache.parquet.schema.MessageType

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import fs2.Chunk
import cats.data.NonEmptyList
import scala.util.control.NonFatal
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import cats.effect.std.Dequeue
import org.apache.parquet.hadoop.ParquetWriter as HadoopParquetWriter

object rotatingWriter {

  val DefaultMaxCount: Long              = org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)
  val DefaultChunkSize                   = 16

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

    /** @return
      *   Builder of pipe that processes data using custom implementation of ParquetWriter
      */
    @experimental
    def custom[T, B <: HadoopParquetWriter.Builder[T, B]](writerBuilderFactory: Path => B): CustomBuilder[F, T]
  }

  private[parquet4s] class ViaParquetImpl[F[_]: Async] extends ViaParquet[F] {
    override def of[T]: TypedBuilder[F, T, T] = TypedBuilderImpl[F, T, T](
      chunkSize              = DefaultChunkSize,
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = t => Stream.emit(t),
      partitionByOpt         = None,
      defaultPartition       = PartialFunction.empty[ColumnPath, String],
      postWriteHandlerOpt    = None,
      writeOptions           = ParquetWriter.Options()
    )
    override def generic: GenericBuilder[F] = GenericBuilderImpl(
      chunkSize              = DefaultChunkSize,
      maxCount               = DefaultMaxCount,
      maxDuration            = DefaultMaxDuration,
      preWriteTransformation = Stream.emit,
      partitionByOpt         = None,
      defaultPartition       = PartialFunction.empty[ColumnPath, String],
      postWriteHandlerOpt    = None,
      writeOptions           = ParquetWriter.Options()
    )

    override def custom[T, B <: HadoopParquetWriter.Builder[T, B]](
        writerBuilderFactory: Path => B
    ): CustomBuilder[F, T] =
      new CustomBuilderImpl(
        chunkSize            = DefaultChunkSize,
        maxCount             = DefaultMaxCount,
        maxDuration          = DefaultMaxDuration,
        partitioning         = (path, t) => (path, t),
        postWriteHandlerOpt  = None,
        writeOptions         = ParquetWriter.Options(),
        writerBuilderFactory = writerBuilderFactory
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

    /** Adds a handler that is invoked after write of each chunk of records. Handler exposes some of the internal state
      * of the flow. Intended for lower level monitoring and control.
      *
      * <br/> If you wish to have postWriteHandler invoked after write of each single element than change the size of
      * chunk by changing a value of `chunkSize` property.
      *
      * @param postWriteHandler
      *   an effect called after writing a chunk of records, receiving a snapshot of the internal state of the flow as a
      *   parameter.
      */
    def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): Self

    /** For sake of better performance writer processes data in chunks rather than one by one. Default value is `16`.
      * @param chunkSize
      *   default value override
      */
    def chunkSize(chunkSize: Int): Self

  }

  trait GenericBuilder[F[_]]
      extends Builder[F, RowParquetRecord, RowParquetRecord, GenericBuilder[F]]
      with ParquetRecordPartitioning[F, RowParquetRecord, GenericBuilder[F]] {

    /** @param transformation
      *   function that is called by stream in order to transform data to final write format. Identity by default.
      */
    def preWriteTransformation(transformation: RowParquetRecord => Stream[F, RowParquetRecord]): GenericBuilder[F]

    /** Builds final writer pipe.
      */
    def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord]
  }

  trait TypedBuilder[F[_], T, W]
      extends Builder[F, T, W, TypedBuilder[F, T, W]]
      with ParquetRecordPartitioning[F, W, TypedBuilder[F, T, W]] {

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

  trait CustomBuilder[F[_], T] extends Builder[F, T, T, CustomBuilder[F, T]] {

    /** @param partitioning
      *   a function which can be used for manipulation of path where the given object T will be written to and the
      *   content of the object before it is written
      * @return
      */
    def partitionUsing(partitioning: (Path, T) => (Path, T)): CustomBuilder[F, T]

    /** Builds final writer pipe.
      */
    def write(basePath: Path): Pipe[F, T, T]
  }

  private case class GenericBuilderImpl[F[_]: Async](
      chunkSize: Int,
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: RowParquetRecord => Stream[F, RowParquetRecord],
      partitionByOpt: Option[NonEmptyList[ColumnPath]],
      defaultPartition: PartialFunction[ColumnPath, String],
      postWriteHandlerOpt: Option[PostWriteHandler[F, RowParquetRecord]],
      writeOptions: ParquetWriter.Options
  ) extends GenericBuilder[F] {
    override def chunkSize(chunkSize: Int): GenericBuilder[F]                    = this.copy(chunkSize = chunkSize)
    override def maxCount(maxCount: Long): GenericBuilder[F]                     = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): GenericBuilder[F]     = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): GenericBuilder[F] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): GenericBuilder[F] =
      copy(partitionByOpt = NonEmptyList.fromList(partitionBy.toList))
    override def defaultPartition(defaultPartition: PartialFunction[ColumnPath, String]): GenericBuilder[F] =
      copy(defaultPartition = defaultPartition)
    override def preWriteTransformation(
        transformation: RowParquetRecord => Stream[F, RowParquetRecord]
    ): GenericBuilder[F] =
      copy(preWriteTransformation = transformation)
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, RowParquetRecord]): GenericBuilder[F] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord] = {

      val encode = (record: RowParquetRecord, _: ValueCodecConfiguration) => Sync[F].pure(record)

      implicit val resolver: ParquetSchemaResolver[RowParquetRecord] =
        RowParquetRecord.genericParquetSchemaResolver(schema)
      val finalSchemaF = Sync[F].catchNonFatal {
        ParquetSchemaResolver.resolveSchema[RowParquetRecord](toSkip =
          partitionByOpt.map(_.toList).getOrElse(Seq.empty)
        )
      }

      rotatingWriter.write[F, RowParquetRecord, RowParquetRecord, RowParquetRecord](
        basePath               = basePath,
        chunkSize              = chunkSize,
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        prewriteTransformation = preWriteTransformation,
        encodeAndPartition     = (_: Path, record: RowParquetRecord) => encodeAndPartition(record, basePath, encode),
        postWriteHandlerOpt    = postWriteHandlerOpt,
        options                = writeOptions,
        createWriter = filePath =>
          finalSchemaF >>= { finalSchema =>
            Sync[F].delay {
              scala.concurrent.blocking {
                ParquetWriter.internalWriter(
                  file           = filePath.toOutputFile(writeOptions),
                  schema         = finalSchema,
                  metadataWriter = MetadataWriter.NoOp,
                  options        = writeOptions
                )
              }
            }
          }
      )
    }

  }

  private case class TypedBuilderImpl[F[_]: Async, T, W](
      chunkSize: Int,
      maxCount: Long,
      maxDuration: FiniteDuration,
      preWriteTransformation: T => Stream[F, W],
      partitionByOpt: Option[NonEmptyList[ColumnPath]],
      defaultPartition: PartialFunction[ColumnPath, String],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      writeOptions: ParquetWriter.Options
  ) extends TypedBuilder[F, T, W] {

    override def chunkSize(chunkSize: Int): TypedBuilder[F, T, W]                    = this.copy(chunkSize = chunkSize)
    override def maxCount(maxCount: Long): TypedBuilder[F, T, W]                     = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): TypedBuilder[F, T, W]     = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): TypedBuilder[F, T, W] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): TypedBuilder[F, T, W] =
      copy(partitionByOpt = NonEmptyList.fromList(partitionBy.toList))
    override def defaultPartition(defaultPartition: PartialFunction[ColumnPath, String]): TypedBuilder[F, T, W] =
      copy(defaultPartition = defaultPartition)
    override def preWriteTransformation[X](transformation: T => Stream[F, X]): TypedBuilder[F, T, X] =
      TypedBuilderImpl(
        chunkSize              = chunkSize,
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        preWriteTransformation = transformation,
        partitionByOpt         = partitionByOpt,
        defaultPartition       = defaultPartition,
        writeOptions           = writeOptions,
        postWriteHandlerOpt    = postWriteHandlerOpt
      )
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): TypedBuilder[F, T, W] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(
        basePath: Path
    )(implicit schemaResolver: ParquetSchemaResolver[W], encoder: ParquetRecordEncoder[W]): Pipe[F, T, T] = {
      val schemaF =
        Sync[F].catchNonFatal(ParquetSchemaResolver.resolveSchema[W](partitionByOpt.map(_.toList).getOrElse(Seq.empty)))

      val encode = (obj: W, vcc: ValueCodecConfiguration) => Sync[F].delay(ParquetRecordEncoder.encode[W](obj, vcc))

      rotatingWriter.write[F, T, W, RowParquetRecord](
        basePath               = basePath,
        chunkSize              = chunkSize,
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        prewriteTransformation = preWriteTransformation,
        encodeAndPartition     = (_: Path, obj: W) => encodeAndPartition(obj, basePath, encode),
        postWriteHandlerOpt    = postWriteHandlerOpt,
        options                = writeOptions,
        createWriter = filePath =>
          schemaF >>= { schema =>
            Sync[F].delay {
              scala.concurrent.blocking {
                ParquetWriter.internalWriter(
                  file           = filePath.toOutputFile(writeOptions),
                  schema         = schema,
                  metadataWriter = MetadataWriter.NoOp,
                  options        = writeOptions
                )
              }
            }
          }
      )
    }
  }

  private case class CustomBuilderImpl[F[_]: Async, T, B <: HadoopParquetWriter.Builder[T, B]](
      chunkSize: Int,
      maxCount: Long,
      maxDuration: FiniteDuration,
      partitioning: (Path, T) => (Path, T),
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      writeOptions: ParquetWriter.Options,
      writerBuilderFactory: Path => B
  ) extends CustomBuilder[F, T] {

    override def chunkSize(chunkSize: Int): CustomBuilder[F, T]                    = this.copy(chunkSize = chunkSize)
    override def maxCount(maxCount: Long): CustomBuilder[F, T]                     = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): CustomBuilder[F, T]     = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): CustomBuilder[F, T] = copy(writeOptions = writeOptions)
    override def partitionUsing(partitioning: (Path, T) => (Path, T)): CustomBuilder[F, T] =
      copy(partitioning = partitioning)
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): CustomBuilder[F, T] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(
        basePath: Path
    ): Pipe[F, T, T] =
      rotatingWriter.write[F, T, T, T](
        basePath               = basePath,
        chunkSize              = chunkSize,
        maxCount               = maxCount,
        maxDuration            = maxDuration,
        prewriteTransformation = Stream.emit,
        encodeAndPartition     = (path: Path, obj: T) => Sync[F].catchNonFatal(partitioning(path, obj)),
        postWriteHandlerOpt    = postWriteHandlerOpt,
        options                = writeOptions,
        createWriter = filePath =>
          Sync[F].delay {
            scala.concurrent.blocking {
              writeOptions.applyTo[T, B](writerBuilderFactory(filePath)).build()
            }
          }
      )
  }

  trait ParquetRecordPartitioning[F[_], W, Self] {

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

    /** Allows to define default partition values for optional or nullable columns.
      * @param defaultPartition
      *   partial function, which Parquet4s will call to attempt to resolve a partition value when it encouters a null
      *   column value
      */
    def defaultPartition(defaultPartition: PartialFunction[ColumnPath, String]): Self

    protected def partitionByOpt: Option[NonEmptyList[ColumnPath]]
    protected def defaultPartition: PartialFunction[ColumnPath, String]
    protected def writeOptions: ParquetWriter.Options

    private lazy val vcc: ValueCodecConfiguration = ValueCodecConfiguration(writeOptions)

    protected def encodeAndPartition(
        valueToWrite: W,
        basePath: Path,
        encode: (W, ValueCodecConfiguration) => F[RowParquetRecord]
    )(implicit F: Sync[F]): F[(Path, RowParquetRecord)] =
      encode(valueToWrite, vcc) >>= (record => partition(record = record, basePath = basePath))

    private def partition(record: RowParquetRecord, basePath: Path)(implicit F: Sync[F]): F[(Path, RowParquetRecord)] =
      partitionByOpt.fold(F.pure(basePath -> record)) { partitionBy =>
        modifyPartitionedRecord(record, partitionBy.head).flatMap {
          case (firstPartitionPath, firstPartitionValue, modifiedRecord) =>
            val builder = new StringBuilder()
            builder.append(firstPartitionPath.toString)
            builder.append("=")
            builder.append(URLEncoder.encode(firstPartitionValue, StandardCharsets.UTF_8.name()))
            partitionRec(modifiedRecord, partitionBy.tail, basePath, builder)
        }
      }

    private def partitionRec(
        record: RowParquetRecord,
        partitionBy: List[ColumnPath],
        basePath: Path,
        builder: StringBuilder
    )(implicit F: Sync[F]): F[(Path, RowParquetRecord)] =
      partitionBy match {
        case columnPath :: rest =>
          modifyPartitionedRecord(record, columnPath).flatMap { case (partitionPath, partitionValue, modifiedRecord) =>
            builder.append(Path.Separator)
            builder.append(partitionPath.toString)
            builder.append("=")
            builder.append(URLEncoder.encode(partitionValue, StandardCharsets.UTF_8.name()))
            partitionRec(modifiedRecord, rest, basePath, builder)
          }
        case Nil =>
          F.pure(Path(basePath, builder.result()) -> record)
      }

    private def modifyPartitionedRecord(
        record: RowParquetRecord,
        partitionColumnPath: ColumnPath
    )(implicit F: Sync[F]): F[(ColumnPath, String, RowParquetRecord)] =
      record.removed(partitionColumnPath) match {
        case (Some(BinaryValue(binary)), modifiedRecord) =>
          F.catchNonFatal((partitionColumnPath, binary.toStringUsingUTF8, modifiedRecord))
        case (Some(NullValue), modifiedRecord) if defaultPartition.isDefinedAt(partitionColumnPath) =>
          F.pure((partitionColumnPath, defaultPartition(partitionColumnPath), modifiedRecord))
        case (Some(NullValue), _) =>
          F.raiseError(new IllegalArgumentException(s"Field '$partitionColumnPath' is null."))
        case (None, _) =>
          F.raiseError(new IllegalArgumentException(s"Field '$partitionColumnPath' does not exist."))
        case _ =>
          F.raiseError(new IllegalArgumentException(s"Non-string field '$partitionColumnPath' used for partitioning."))
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
  case class PostWriteState[F[_], T](
      processedData: Chunk[T],
      modifiedPartitions: Map[Path, Long],
      flush: Path => F[Unit]
  )

  sealed private trait WriterEvent[F[_], T, W]
  private case class DataEvent[F[_], T, W](data: Stream[F, W], out: T) extends WriterEvent[F, T, W]
  private case class RotateEvent[F[_], T, W](partition: Path) extends WriterEvent[F, T, W]
  private case class StopEvent[F[_], T, W]() extends WriterEvent[F, T, W]

  private object RecordWriter {

    private def newFileName(options: ParquetWriter.Options): String = {
      val compressionExtension = options.compressionCodecName.getExtension
      UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    def apply[F[_], T, W, R](
        basePath: Path,
        createWriter: Path => F[HadoopParquetWriter[R]],
        options: ParquetWriter.Options,
        eventDequeue: Dequeue[F, WriterEvent[F, T, W]],
        maxDuration: FiniteDuration
    )(implicit F: Async[F]): F[RecordWriter[F, R]] =
      F.uncancelable { _ =>
        for {
          // internalWriter <- F.delay(
          //   scala.concurrent.blocking(
          //     ParquetWriter.internalWriter(
          //       file           = basePath.append(newFileName(options)).toOutputFile(options),
          //       schema         = schema,
          //       metadataWriter = MetadataWriter.NoOp,
          //       options        = options
          //     )
          //   )
          // )
          internalWriter <- createWriter(basePath.append(newFileName(options)))
          rotationFiber  <- F.delayBy(eventDequeue.offerFront(RotateEvent[F, T, W](basePath)), maxDuration).start
        } yield new RecordWriter(internalWriter, rotationFiber)
      }
  }

  private class RecordWriter[F[_], R](
      internalWriter: HadoopParquetWriter[R],
      rotationFiber: Fiber[F, Throwable, Unit]
  )(implicit F: Async[F]) {

    var count: Long       = 0
    var disposed: Boolean = false

    def write(record: R): F[Long] = F.delay(scala.concurrent.blocking {
      internalWriter.write(record)
      count = count + 1
      count
    })

    def dispose: F[Unit] =
      F.uncancelable { _ =>
        disposed = true
        rotationFiber.cancel >> F.delay(scala.concurrent.blocking(internalWriter.close())).recover { case NonFatal(_) =>
          () // ignores bug in Parquet
        }
      }
  }

  private class RotatingWriter[F[_], T, W, R](
      basePath: Path,
      options: ParquetWriter.Options,
      chunkSize: Int,
      maxCount: Long,
      maxDuration: FiniteDuration,
      encodeAndPartition: (Path, W) => F[(Path, R)],
      eventDequeue: Dequeue[F, WriterEvent[F, T, W]],
      logger: Logger[F],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      createWriter: Path => F[HadoopParquetWriter[R]],
      writersRef: Ref[F, Map[Path, RecordWriter[F, R]]]
  )(implicit F: Async[F]) {

    private def write(chunk: Chunk[W]): F[Map[Path, Long]] =
      chunk.foldM(Map.empty[Path, Long]) { case (map, entity) =>
        write(entity).map { case (path, count) =>
          map.updated(path, count)
        }
      }

    private def write(entity: W): F[(Path, Long)] =
      for {
        partitioning <- encodeAndPartition(basePath, entity)
        (path, partitionedRecord) = partitioning
        count <- writersRef.access.flatMap { case (writers, setter) =>
          writers.get(path) match {
            // it should never happened that disposed writer is left in the map but let's be safe
            case Some(writer) if !writer.disposed =>
              for {
                count <- writer.write(partitionedRecord)
                _ <-
                  if (count >= maxCount) {
                    writer.dispose >> setter(MapCompat.remove(writers, path)).void
                  } else {
                    F.unit
                  }
              } yield count
            case _ =>
              for {
                writer <- RecordWriter[F, T, W, R](path, createWriter, options, eventDequeue, maxDuration)
                count  <- writer.write(partitionedRecord)
                isUpdated <-
                  if (count >= maxCount) {
                    // writer is not supposed to be added to the map, but disposed immediately
                    F.pure(false)
                  } else {
                    setter(writers.updated(path, writer))
                  }
                _ <-
                  if (isUpdated) {
                    F.unit
                  } else {
                    // updating ref didn't succeed or writer is intended to be disposed immediately
                    writer.dispose
                  }
              } yield count
          }
        }
      } yield path -> count

    private def disposeAll: F[Unit] =
      F.uncancelable { _ =>
        for {
          writers <- writersRef.getAndSet(Map.empty)
          _       <- writers.values.toList.traverse_(_.dispose)
        } yield ()
      }

    private def dispose(partition: Path): F[Unit] =
      F.uncancelable { _ =>
        for {
          removedWriterOpt <- writersRef.modify { writers =>
            writers.get(partition) match {
              case Some(writer) =>
                MapCompat.remove(writers, partition) -> Some(writer)
              case None =>
                writers -> None
            }
          }
          _ <- removedWriterOpt.traverse_(_.dispose)
        } yield ()
      }

    private def rotatePull(partitions: Iterable[Path]): Pull[F, T, Unit] =
      partitions
        .map(partition => Pull.eval(logger.debug(s"Rotating $partition")) >> Pull.eval(dispose(partition)))
        .reduceOption(_ >> _)
        .getOrElse(Pull.done)

    private def postWriteHandlerPull(
        out: Chunk[T],
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

    private def writeEntitiesAndOutputPull(
        entityStream: Stream[F, W],
        outChunk: Chunk[T]
    ): Pull[F, T, Map[Path, Long]] =
      writeEntityChunksAndOutputPull(
        entityChunksStream = entityStream.chunkN(chunkSize),
        outChunk           = outChunk,
        modifiedPartitions = Map.empty
      )

    private def writeEntityChunksAndOutputPull(
        entityChunksStream: Stream[F, Chunk[W]],
        outChunk: Chunk[T],
        modifiedPartitions: Map[Path, Long]
    ): Pull[F, T, Map[Path, Long]] =
      entityChunksStream.pull.uncons1.flatMap {
        case Some((chunk, tail)) =>
          Pull.eval(write(chunk)).flatMap { chunkModifiedPartitions =>
            writeEntityChunksAndOutputPull(tail, outChunk, modifiedPartitions ++ chunkModifiedPartitions)
          }
        case None if postWriteHandlerOpt.isEmpty =>
          Pull.output(outChunk) >> Pull.pure(modifiedPartitions)
        case None =>
          postWriteHandlerPull(outChunk, modifiedPartitions).flatMap {
            case Nil =>
              Pull.output(outChunk) >> Pull.pure(modifiedPartitions)
            case partitionsToRotate =>
              rotatePull(partitionsToRotate) >> Pull.output(outChunk) >> Pull.pure(modifiedPartitions)
          }
      }

    sealed private trait Acc
    private case class DataAcc(data: Stream[F, W], chunk: Chunk[T], pull: Pull[F, T, Unit]) extends Acc
    private case class StopAcc(data: Stream[F, W], chunk: Chunk[T], pull: Pull[F, T, Unit]) extends Acc

    private def writeAllEventsPull(in: Stream[F, Chunk[WriterEvent[F, T, W]]]): Pull[F, T, Unit] =
      in.pull.uncons1.flatMap {
        case Some((eventChunk, tail)) =>
          eventChunk.foldLeft[Acc](DataAcc(Stream.empty, Chunk.empty, Pull.done)) {
            case (DataAcc(dataStream, outChunk, pull), DataEvent(data, out)) =>
              DataAcc(dataStream ++ data, outChunk.appendK(out), pull)
            case (DataAcc(dataStream, outChunk, pull), RotateEvent(partition)) =>
              DataAcc(
                Stream.empty,
                Chunk.empty[T],
                pull >> writeEntitiesAndOutputPull(dataStream, outChunk) >> rotatePull(Iterable(partition))
              )
            case (DataAcc(dataStream, outChunk, pull), StopEvent()) =>
              StopAcc(dataStream, outChunk, pull)
            case (stop: StopAcc, _) =>
              stop
          } match {
            case StopAcc(_, outChunk, pull) if outChunk.isEmpty =>
              pull
            case StopAcc(dataStream, outChunk, pull) =>
              pull >> writeEntitiesAndOutputPull(dataStream, outChunk) >> Pull.done
            case DataAcc(_, outChunk, pull) if outChunk.isEmpty =>
              pull >> writeAllEventsPull(tail)
            case DataAcc(dataStream, outChunk, pull) =>
              pull >> writeEntitiesAndOutputPull(dataStream, outChunk) >> writeAllEventsPull(tail)
          }
        case None =>
          Pull.done
      }

    def writeAllEvents(in: Stream[F, WriterEvent[F, T, W]]): Stream[F, T] =
      writeAllEventsPull(in.chunkLimit(chunkSize)).stream.onFinalize(disposeAll)
  }

  private def write[F[_], T, W, R](
      basePath: Path,
      chunkSize: Int,
      maxCount: Long,
      maxDuration: FiniteDuration,
      prewriteTransformation: T     => Stream[F, W],
      encodeAndPartition: (Path, W) => F[(Path, R)],
      postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
      options: ParquetWriter.Options,
      createWriter: Path => F[HadoopParquetWriter[R]]
  )(implicit F: Async[F]): Pipe[F, T, T] =
    in =>
      for {
        logger       <- Stream.eval(logger[F](this.getClass))
        _            <- Stream.eval(io.validateWritePath[F](basePath, options, logger))
        eventDequeue <- Stream.eval(Dequeue.unbounded[F, WriterEvent[F, T, W]])
        writersRef   <- Stream.eval(Ref.of(Map.empty[Path, RecordWriter[F, R]]))
        rotatingWriter <- Stream.emit(
          new RotatingWriter[F, T, W, R](
            basePath            = basePath,
            options             = options,
            chunkSize           = chunkSize,
            maxCount            = maxCount,
            maxDuration         = maxDuration,
            encodeAndPartition  = encodeAndPartition,
            eventDequeue        = eventDequeue,
            logger              = logger,
            postWriteHandlerOpt = postWriteHandlerOpt,
            createWriter        = createWriter,
            writersRef          = writersRef
          )
        )
        eventStream = Stream(
          Stream.fromQueueUnterminated(eventDequeue, limit = chunkSize),
          in
            .map { inputElement =>
              DataEvent[F, T, W](prewriteTransformation(inputElement), inputElement)
            }
            .append(Stream.emit(StopEvent[F, T, W]()))
        ).parJoin(maxOpen = 2)
        out <- rotatingWriter.writeAllEvents(eventStream)
      } yield out

}
