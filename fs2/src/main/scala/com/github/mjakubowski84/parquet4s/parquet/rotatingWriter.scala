package com.github.mjakubowski84.parquet4s.parquet

import cats.effect._
import cats.implicits._
import com.github.mjakubowski84.parquet4s._
import com.github.mjakubowski84.parquet4s.parquet.logger.Logger
import fs2.{Pipe, Pull, Stream}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetWriter => HadoopParquetWriter}
import org.apache.parquet.schema.MessageType

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

object rotatingWriter {

  object Builder {

    val DefaultMaxCount: Long = HadoopParquetWriter.DEFAULT_BLOCK_SIZE
    val DefaultMaxDuration: FiniteDuration = FiniteDuration(1, TimeUnit.MINUTES)

    private[parquet4s] def apply[F[_], T](): Builder[F, T, T] = BuilderImpl[F, T, T](
      maxCount = DefaultMaxCount,
      maxDuration = DefaultMaxDuration,
      preWriteTransformation = t => Stream.emit(t),
      partitionBy = Seq.empty,
      postWriteHandlerOpt = None,
      writeOptions = ParquetWriter.Options()
    )

  }

  trait Builder[F[_], T, W] {
    /**
     * @param maxCount max number of records to be written before file rotation
     */
    def maxCount(maxCount: Long): Builder[F, T, W]
    /**
     * @param maxDuration max time after which partition file is rotated
     */
    def maxDuration(maxDuration: FiniteDuration): Builder[F, T, W]
    /**
     * @param writeOptions writer options used by the flow
     */
    def options(writeOptions: ParquetWriter.Options): Builder[F, T, W]
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
     *   <li>Partitioning removes partition fields from the schema. Data is stored in name of subdirectory
     *       instead of Parquet file.</li>
     *   <li>Partitioning cannot end in having empty schema. If you remove all fields of the message you will
     *       get an error.</li>
     *   <li>Partitioned directories can be filtered effectively during reading.</li>
     * </ol>
     *
     * @param partitionBy [[ColumnPath]]s to partition by
     */
    def partitionBy(partitionBy: ColumnPath*): Builder[F, T, W]
    /**
     * @param transformation function that is called by stream in order to obtain Parquet schema. Identity by default.
     * @tparam X Schema type
     */
    def preWriteTransformation[X](transformation: T => Stream[F, X]): Builder[F, T, X]

    /**
     * Adds a handler after record writes, exposing some of the internal state of the flow.
     * Intended for lower level monitoring and control.
     *
     * @param postWriteHandler an effect called after writing a record,
     *                         receiving a snapshot of the internal state of the flow as a parameter.
     */
    def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): Builder[F, T, W]
    /**
     * Builds final writer pipe.
     */
    def write(basePath: String)(implicit
                                schemaResolver: ParquetSchemaResolver[W],
                                encoder: ParquetRecordEncoder[W],
                                async: Async[F]): Pipe[F, T, T]
  }

  private case class BuilderImpl[F[_], T, W](
                                              maxCount: Long,
                                              maxDuration: FiniteDuration,
                                              preWriteTransformation: T => Stream[F, W],
                                              partitionBy: Seq[ColumnPath],
                                              postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
                                              writeOptions: ParquetWriter.Options,
                                            ) extends Builder[F, T, W] {

    override def maxCount(maxCount: Long): Builder[F, T, W] = copy(maxCount = maxCount)
    override def maxDuration(maxDuration: FiniteDuration): Builder[F, T, W] = copy(maxDuration = maxDuration)
    override def options(writeOptions: ParquetWriter.Options): Builder[F, T, W] = copy(writeOptions = writeOptions)
    override def partitionBy(partitionBy: ColumnPath*): Builder[F, T, W] = copy(partitionBy = partitionBy)
    override def preWriteTransformation[X](transformation: T => Stream[F, X]): Builder[F, T, X] =
      BuilderImpl(
        maxCount = maxCount,
        maxDuration = maxDuration,
        preWriteTransformation = transformation,
        partitionBy = partitionBy,
        writeOptions = writeOptions,
        postWriteHandlerOpt = postWriteHandlerOpt
      )
    override def postWriteHandler(postWriteHandler: PostWriteHandler[F, T]): Builder[F, T, W] =
      copy(postWriteHandlerOpt = Option(postWriteHandler))
    override def write(basePath: String)(implicit
                                         schemaResolver: ParquetSchemaResolver[W],
                                         encoder: ParquetRecordEncoder[W],
                                         async: Async[F]): Pipe[F, T, T] =
      rotatingWriter.write[F, T, W](
        basePath, maxCount, maxDuration, partitionBy, preWriteTransformation, postWriteHandlerOpt, writeOptions
      )
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
                                           writersRef: Ref[F, Map[Path, RecordWriter[F]]],
                                           maxCount: Long,
                                           partitionBy: List[ColumnPath],
                                           schema: MessageType,
                                           encode: W => F[RowParquetRecord],
                                           logger: Logger[F],
                                           postWriteHandlerOpt: Option[PostWriteHandler[F, T]]
                                          )(implicit F: Sync[F]) {

    private type Writers = Map[Path, RecordWriter[F]]

    private def writePull(entity: W, writers: Writers): Pull[F, T, Writers] =
      Pull.eval(write(entity, writers))

    private def newFileName: String = {
      val compressionExtension = options.compressionCodecName.getExtension
      UUID.randomUUID().toString + compressionExtension + ".parquet"
    }

    private def getOrCreateWriter(path: Path, writers: Writers): F[(RecordWriter[F], Writers)] =
      writers.get(path) match {
        case Some(writer) =>
          F.pure(writer -> writers)
        case None =>
          for {
            writer <- RecordWriter(new Path(path, newFileName), schema, options)
            updatedWriters = writers.updated(path, writer)
            _ <- writersRef.set(updatedWriters)
          } yield writer -> updatedWriters
      }

    private def write(entity: W, writers: Writers): F[Writers] =
      for {
        record <- encode(entity)
        path <- partitionBy.traverse(partition(record)).map {
          partitions => partitions.foldLeft(basePath) {
            case (path, (partitionName, partitionValue)) => new Path(path, s"$partitionName=$partitionValue")
          }
        }
        getResult <- getOrCreateWriter(path, writers)
        (writer, writers) = getResult
        _ <- writer.write(record)
      } yield writers

    private def partition(record: RowParquetRecord)(partitionPath: ColumnPath): F[(ColumnPath, String)] = {
      record.remove(partitionPath) match {
        case None => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' does not exist."))
        case Some(NullValue) => F.raiseError(new IllegalArgumentException(s"Field '$partitionPath' is null."))
        case Some(BinaryValue(binary)) => F.catchNonFatal(partitionPath -> binary.toStringUsingUTF8)
        case _ => F.raiseError(new IllegalArgumentException(s"Non-string field '$partitionPath' used for partitioning."))
      }
    }

    // TODO consider using Hotswap
    private def dispose: F[Unit] =
      for {
        writers <- writersRef.modify(writers => (Map.empty, writers.values))
        _ <- Stream.iterable(writers).evalMap(_.dispose).compile.drain
      } yield ()

    private def rotatePull(reason: String): Pull[F, T, Unit] =
      Pull.eval(logger.debug(s"Rotating on $reason")) >> Pull.eval(dispose)

    private def postWriteHandlerPull(out: T,
                                     count: Long,
                                     partitions: Iterable[Path]
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

    private def writeAllPull(in: Stream[F, WriterEvent[T, W]], writers: Writers, count: Long): Pull[F, T, Unit] = {
      in.pull.uncons1.flatMap {
        case Some((DataEvent(data), tail)) if count < maxCount =>
          writePull(data, writers).flatMap(updatedWriters => writeAllPull(tail, updatedWriters, count + 1))
        case Some((DataEvent(data), tail)) =>
          writePull(data, writers) >> rotatePull("max count reached") >> writeAllPull(tail, writers = Map.empty, count = 0)
        case Some((RotateEvent(reason), tail)) =>
          rotatePull(reason) >> writeAllPull(tail, writers = Map.empty, count = 0)
        case Some((OutputEvent(out), tail)) if postWriteHandlerOpt.isEmpty =>
          Pull.output1(out) >> writeAllPull(tail, writers, count)
        case Some((OutputEvent(out), tail)) =>
          Pull.output1(out) >> postWriteHandlerPull(out, count, writers.keys).flatMap {
            case None => writeAllPull(tail, writers, count)
            case Some(rotate) => writeAllPull(Stream.emit(rotate) ++ tail, writers, count)
          }
        case _ =>
          Pull.done
      }
    }

    def writeAll(in: Stream[F, WriterEvent[T, W]]): Stream[F, T] =
      writeAllPull(in, writers = Map.empty, count = 0).stream.onFinalize(dispose)
  }

  private[parquet4s] def write[
    F[_],
    T,
    W: ParquetRecordEncoder : ParquetSchemaResolver](path: String,
                                                     maxCount: Long,
                                                     maxDuration: FiniteDuration,
                                                     partitionBy: Seq[ColumnPath],
                                                     prewriteTransformation: T => Stream[F, W],
                                                     postWriteHandlerOpt: Option[PostWriteHandler[F, T]],
                                                     options: ParquetWriter.Options
                                                    )(implicit F: Async[F]): Pipe[F, T, T] =
    in =>
      for {
        hadoopPath <- Stream.eval(io.makePath(path))
        schema <- Stream.eval(F.catchNonFatal(ParquetSchemaResolver.resolveSchema[W](partitionBy)))
        valueCodecConfiguration <- Stream.eval(F.catchNonFatal(options.toValueCodecConfiguration))
        encode = { (entity: W) => F.catchNonFatal(ParquetRecordEncoder.encode[W](entity, valueCodecConfiguration)) }
        logger <- Stream.eval(logger[F](this.getClass))
        rotatingWriter <- Stream.emit(
          new RotatingWriter[T, W, F](
            basePath = hadoopPath,
            options = options,
            writersRef = Ref.unsafe(Map.empty),
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
