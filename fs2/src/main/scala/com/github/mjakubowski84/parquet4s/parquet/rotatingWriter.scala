package com.github.mjakubowski84.parquet4s.parquet

import cats.effect.*
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

  val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
  val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)

  trait ViaParquet[F[_]] {
    /**
     * @tparam T schema type
     * @return Builder of pipe that processes data of specified schema
     */
    def of[T]: TypedBuilder[F, T, T]
    /**
     * @return Builder of pipe that processes generic records
     */
    def generic: GenericBuilder[F]
  }

  private[parquet4s] class ViaParquetImpl[F[_]: Async] extends ViaParquet[F] {
    override def of[T]: TypedBuilder[F, T, T] = TypedBuilderImpl[F, T, T](
      maxCount = DefaultMaxCount,
      maxDuration = DefaultMaxDuration,
      preWriteTransformation = t => Stream.emit(t),
      partitionBy = Seq.empty,
      postWriteHandlerOpt = None,
      writeOptions = ParquetWriter.Options()
    )
    override def generic: GenericBuilder[F] = GenericBuilderImpl(
      maxCount = DefaultMaxCount,
      maxDuration = DefaultMaxDuration,
      preWriteTransformation = Stream.emit,
      partitionBy = Seq.empty,
      postWriteHandlerOpt = None,
      writeOptions = ParquetWriter.Options()
    )
  }

  trait Builder[F[_], T, W, Self] {
    /**
     * @param maxCount max number of records to be written before file rotation
     */
    def maxCount(maxCount: Long): Self
    /**
     * @param maxDuration max time after which partition file is rotated
     */
    def maxDuration(maxDuration: FiniteDuration): Self
    /**
     * @param writeOptions writer options used by the flow
     */
    def options(writeOptions: ParquetWriter.Options): Self
    /**
     * Sets partition paths that stream partitions data by. Can be empty.
     * Partition path can be a simple string column (e.g. "color") or a path pointing nested string field
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
     *   <li>Partitioning removes partition fields from the schema. Data is stored in the name of subdirectory
     *       instead of Parquet file.</li>
     *   <li>Partitioning cannot end in having empty schema. If you remove all fields of the message you will
     *       get an error.</li>
     *   <li>Partitioned directories can be filtered effectively during reading.</li>
     * </ol>
     *
     * @param partitionBy [[ColumnPath]]s to partition by
     */
    def partitionBy(partitionBy: ColumnPath*): Self

    /**
     * Adds a handler after record writes, exposing some of the internal state of the flow.
     * Intended for lower level monitoring and control.
     *
     * @param postWriteHandler an effect called after writing a record,
     *                         receiving a snapshot of the internal state of the flow as a parameter.
     */
    def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): Self

  }

  trait GenericBuilder[F[_]] extends Builder[F, RowParquetRecord, RowParquetRecord, GenericBuilder[F]] {
    /**
     * @param transformation function that is called by stream in order to transform data to final write format. Identity by default.
     */
    def preWriteTransformation(transformation: RowParquetRecord => Stream[F, RowParquetRecord]): GenericBuilder[F]
    /**
     * Builds final writer pipe.
     */
    def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord]
  }

  trait TypedBuilder[F[_], T, W] extends Builder[F, T, W, TypedBuilder[F, T, W]] {
    /**
     * @param transformation function that is called by stream in order to transform data to final write format. Identity by default.
     * @tparam X Schema type
     */
    def preWriteTransformation[X](transformation: T => Stream[F, X]): TypedBuilder[F, T, X]
    /**
     * Builds final writer pipe.
     */
    def write(basePath: Path)(implicit
                              schemaResolver: ParquetSchemaResolver[W],
                              encoder: ParquetRecordEncoder[W]
                              ): Pipe[F, T, T]
  }

  private case class GenericBuilderImpl[F[_]: Async](maxCount: Long,
                                                     maxDuration: FiniteDuration,
                                                     preWriteTransformation: RowParquetRecord => Stream[F, RowParquetRecord],
                                                     partitionBy: Seq[ColumnPath],
                                                     postWriteHandlerOpt: Option[PostWriteHandler[F, RowParquetRecord]],
                                                     writeOptions: ParquetWriter.Options
                                                    ) extends GenericBuilder[F] {
    override def maxCount(maxCount: Long): GenericBuilder[F] = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): GenericBuilder[F] = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): GenericBuilder[F] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): GenericBuilder[F] = copy(partitionBy = partitionBy)
    override def preWriteTransformation(transformation: RowParquetRecord => Stream[F, RowParquetRecord]): GenericBuilder[F] =
      copy(preWriteTransformation = transformation)
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, RowParquetRecord]): GenericBuilder[F] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    def write(basePath: Path, schema: MessageType): Pipe[F, RowParquetRecord, RowParquetRecord] =
      rotatingWriter.write[F, RowParquetRecord, RowParquetRecord](
        basePath, Async[F].pure(schema), maxCount, maxDuration, partitionBy, preWriteTransformation, postWriteHandlerOpt, writeOptions
      )
  }

  private case class TypedBuilderImpl[F[_]: Async, T, W](maxCount: Long,
                                                         maxDuration: FiniteDuration,
                                                         preWriteTransformation: T => Stream[F, W],
                                                         partitionBy: Seq[ColumnPath],
                                                         postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
                                                         writeOptions: ParquetWriter.Options
                                                        ) extends TypedBuilder[F, T, W] {

    override def maxCount(maxCount: Long): TypedBuilder[F, T, W] = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): TypedBuilder[F, T, W] = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): TypedBuilder[F, T, W] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): TypedBuilder[F, T, W] = copy(partitionBy = partitionBy)
    override def preWriteTransformation[X](transformation: T => Stream[F, X]): TypedBuilder[F, T, X] =
      TypedBuilderImpl(
        maxCount = maxCount,
        maxDuration = maxDuration,
        preWriteTransformation = transformation,
        partitionBy = partitionBy,
        writeOptions = writeOptions,
        postWriteHandlerOpt = postWriteHandlerOpt
      )
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): TypedBuilder[F, T, W] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(basePath: Path)(implicit
                                       schemaResolver: ParquetSchemaResolver[W],
                                       encoder: ParquetRecordEncoder[W]): Pipe[F, T, T] = {
      val schemaF = Sync[F].catchNonFatal(ParquetSchemaResolver.resolveSchema[W](partitionBy))
      rotatingWriter.write[F, T, W](
        basePath, schemaF, maxCount, maxDuration, partitionBy, preWriteTransformation, postWriteHandlerOpt, writeOptions
      )
    }
  }

  type PostWriteHandler[F[_], T] = PostWriteState[F, T] => F[Unit]
  case class PostWriteState[F[_], T](count: Long,
                                     lastProcessed: T,
                                     partitions: Iterable[Path],
                                     flush: F[Unit]
                                    )

  private sealed trait WriterEvent[T, W]
  private case class DataEvent[T, W](data: W) extends WriterEvent[T, W]
  private case class RotateEvent[T, W](reason: String) extends WriterEvent[T, W]
  private case class OutputEvent[T, W](out: T) extends WriterEvent[T, W]
  private case class StopEvent[T, W]() extends WriterEvent[T, W]

  private object RecordWriter {
    def apply[F[_]](path: Path,
                    schema: MessageType,
                    options: ParquetWriter.Options
                   )(implicit F: Sync[F]): F[RecordWriter[F]] =
      F.blocking(ParquetWriter.internalWriter(path, schema, options)).map(iw => new RecordWriter(iw))
  }

  private class RecordWriter[F[_]: Sync](internalWriter: ParquetWriter.InternalWriter)(implicit F: Sync[F]) {

    def write(record: RowParquetRecord): F[Unit] = F.blocking(internalWriter.write(record))

    def dispose: F[Unit] = F.blocking(internalWriter.close())

  }

  private class RotatingWriter[T, W, F[_]](basePath: Path,
                                           options: ParquetWriter.Options,
                                           maxCount: Long,
                                           partitionBy: List[ColumnPath],
                                           schema: MessageType,
                                           encode: W => F[RowParquetRecord],
                                           logger: Logger[F],
                                           postWriteHandlerOpt: Option[PostWriteHandler[F, T]]
                                          )(implicit F: Sync[F]) {

    private val writers = TrieMap.empty[Path, RecordWriter[F]]

    private def writePull(entity: W): Pull[F, T, Unit] =
      Pull.eval(write(entity))

    private def newFileName: String = {
      val compressionExtension = options.compressionCodecName.getExtension
      UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    private def getOrCreateWriter(basePath: Path): F[RecordWriter[F]] = {
      writers.get(basePath) match {
          case Some(writer) =>
            F.pure(writer)
          case None =>
            for {
              writer <- RecordWriter(basePath.append(newFileName), schema, options)
              existingWriterOpt <- F.delay { writers.putIfAbsent(basePath, writer) }
              resultingWriter <- existingWriterOpt match {
                case Some(existing) => writer.dispose.as(existing)
                case None => F.pure(writer)
              }
            } yield resultingWriter
        }
    }

    private def write(entity: W): F[Unit] =
      for {
        record <- encode(entity)
        partitioning <- partitionBy.foldLeft(F.pure(basePath -> record)) {
          case (f, currentPartition) =>
            f.flatMap { case (currentPath, currentRecord) =>
              partition(currentRecord, currentPartition).map { case (partitionPath, partitionValue, modifiedRecord) =>
                currentPath.append(s"$partitionPath=$partitionValue") -> modifiedRecord
              }
            }
        }
        (path, partitionedRecord) = partitioning
        writer <- getOrCreateWriter(path)
        _ <- writer.write(partitionedRecord)
      } yield ()

    private def partition(record: RowParquetRecord, partitionPath: ColumnPath): F[(ColumnPath, String, RowParquetRecord)] = {
      record.removed(partitionPath) match {
        case (None, _) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' does not exist."))
        case (Some(NullValue), _) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' is null."))
        case (Some(BinaryValue(binary)), modifiedRecord) => F.catchNonFatal((partitionPath, binary.toStringUsingUTF8, modifiedRecord))
        case _ => F.raiseError(new IllegalArgumentException(s"Non-string field '$partitionPath' used for partitioning."))
      }
    }

    private def dispose: F[Unit] =
      Stream
        .suspend(Stream.iterable(writers.values))
        .evalMap(_.dispose)
        .onFinalize(F.delay { writers.clear() })
        .compile.drain

    private def rotatePull(reason: String): Pull[F, T, Unit] =
      Pull.eval(logger.debug(s"Rotating on $reason")) >> Pull.eval(dispose)

    private def postWriteHandlerPull(out: T,
                                     count: Long,
                                     partitions: => Iterable[Path]
                                    ): Pull[F, T, Option[WriterEvent[T, W]]] =
      postWriteHandlerOpt.fold(Pull.eval[F, Option[WriterEvent[T, W]]](F.pure(None)))(handler => Pull.eval {
        for {
          eventRef <- Ref.of[F, Option[WriterEvent[T, W]]](None)
          state = PostWriteState[F, T](
            count = count,
            lastProcessed = out,
            partitions = partitions,
            flush = eventRef.set(Some(RotateEvent("flush on request")))
          )
          _ <- handler(state)
          eventOpt <- eventRef.get
        } yield eventOpt
      })


    private def writeAllPull(in: Stream[F, WriterEvent[T, W]], count: Long): Pull[F, T, Unit] = {
      in.pull.uncons1.flatMap {
        case Some((DataEvent(data), tail)) if count < maxCount =>
          writePull(data) >> writeAllPull(tail, count + 1)
        case Some((DataEvent(data), tail)) =>
          writePull(data) >> rotatePull("max count reached") >> writeAllPull(tail, count = 0)
        case Some((RotateEvent(reason), tail)) =>
          rotatePull(reason) >> writeAllPull(tail, count = 0)
        case Some((OutputEvent(out), tail)) if postWriteHandlerOpt.isEmpty =>
          Pull.output1(out) >> writeAllPull(tail, count)
        case Some((OutputEvent(out), tail)) =>
          Pull.output1(out) >> postWriteHandlerPull(out, count, writers.keys).flatMap {
            case None => writeAllPull(tail, count)
            case Some(rotate) => writeAllPull(Stream.emit(rotate) ++ tail, count)
          }
        case _ =>
          Pull.done
      }
    }

    def writeAll(in: Stream[F, WriterEvent[T, W]]): Stream[F, T] =
      writeAllPull(in, count = 0).stream.onFinalize(dispose)
  }

  private def write[F[_], T, W: ParquetRecordEncoder](basePath: Path,
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
        schema <- Stream.eval(schemaF)
        valueCodecConfiguration <- Stream.eval(F.catchNonFatal(ValueCodecConfiguration(options)))
        encode = { (entity: W) => F.catchNonFatal(ParquetRecordEncoder.encode[W](entity, valueCodecConfiguration)) }
        logger <- Stream.eval(logger[F](this.getClass))
        rotatingWriter <- Stream.emit(
          new RotatingWriter[T, W, F](
            basePath = basePath,
            options = options,
            maxCount = maxCount,
            partitionBy = partitionBy.toList,
            schema = schema,
            encode = encode,
            logger = logger,
            postWriteHandlerOpt = postWriteHandlerOpt
          )
        )
        eventStream = Stream(
          Stream.awakeEvery[F](maxDuration).map[WriterEvent[T, W]](_ => RotateEvent[T, W]("write timeout")),
          in
            .flatMap { inputElement =>
              prewriteTransformation(inputElement)
                .map(DataEvent.apply[T, W])
                .append(Stream.emit(OutputEvent[T, W](inputElement)))
            }
            .append(Stream.emit(StopEvent[T, W]()))
        ).parJoin(maxOpen = 2)
        out <- rotatingWriter.writeAll(eventStream)
      } yield out

}
