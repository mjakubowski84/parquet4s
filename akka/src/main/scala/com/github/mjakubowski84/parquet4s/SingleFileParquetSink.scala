package com.github.mjakubowski84.parquet4s

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.parquet.hadoop.ParquetWriter as HadoopParquetWriter
import org.apache.parquet.io.OutputFile
import org.apache.parquet.schema.MessageType
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.Try
import scala.util.control.NonFatal

object SingleFileParquetSink {

  trait ToParquet {

    /** Creates a builder of pipe that processes data of given type
      * @tparam T
      *   Schema type
      */
    def of[T: ParquetSchemaResolver: ParquetRecordEncoder]: Builder[T]

    /** Creates a builder of pipe that processes generic records
      */
    def generic(schema: MessageType): Builder[RowParquetRecord]

    /** Creates a builder of a sink that processes data of a given type using a
      * [[org.apache.parquet.hadoop.ParquetWriter]] built from a provided
      * [[org.apache.parquet.hadoop.ParquetWriter.Builder]].
      * @tparam T
      *   Schema type
      * @tparam B
      *   Type of custom [[org.apache.parquet.hadoop.ParquetWriter.Builder]]
      */
    @experimental
    def custom[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): CustomBuilder[T]
  }

  private[parquet4s] object ToParquetImpl extends ToParquet {
    override def of[T: ParquetSchemaResolver: ParquetRecordEncoder]: Builder[T] =
      BuilderImpl()
    override def generic(schema: MessageType): Builder[RowParquetRecord] =
      BuilderImpl()(
        schemaResolver = RowParquetRecord.genericParquetSchemaResolver(schema),
        encoder        = RowParquetRecord.genericParquetRecordEncoder
      )
    override def custom[T, B <: HadoopParquetWriter.Builder[T, B]](builder: B): CustomBuilder[T] =
      CustomBuilderImpl(builder)
  }

  trait Builder[T] {

    /** @param options
      *   writer options
      */
    def options(options: ParquetWriter.Options): Builder[T]

    /** @param file
      *   at which data is supposed to be written
      * @return
      *   final [[akka.stream.scaladsl.Sink]]
      */
    def write(file: OutputFile): Sink[T, Future[Done]]

    /** @param path
      *   at which data is supposed to be written
      * @return
      *   final [[akka.stream.scaladsl.Sink]]
      */
    def write(path: Path): Sink[T, Future[Done]]
  }

  private case class BuilderImpl[T](options: ParquetWriter.Options = ParquetWriter.Options())(implicit
      schemaResolver: ParquetSchemaResolver[T],
      encoder: ParquetRecordEncoder[T]
  ) extends Builder[T] {
    override def options(options: ParquetWriter.Options): Builder[T] = this.copy(options = options)
    override def write(file: OutputFile): Sink[T, Future[Done]]      = rowParquetRecordSink(file, options)
    override def write(path: Path): Sink[T, Future[Done]] = rowParquetRecordSink(path.toOutputFile(options), options)
  }

  trait CustomBuilder[T] {

    /** @param options
      *   writer options
      */
    def options(options: ParquetWriter.Options): CustomBuilder[T]

    /** @return
      *   final [[akka.stream.scaladsl.Sink]]
      */
    def write: Sink[T, Future[Done]]
  }

  private case class CustomBuilderImpl[T, B <: HadoopParquetWriter.Builder[T, B]](
      builder: B,
      maybeOptions: Option[ParquetWriter.Options] = None
  ) extends CustomBuilder[T] {
    override def options(options: ParquetWriter.Options): CustomBuilder[T] =
      this.copy(maybeOptions = Some(options))

    override def write: Sink[T, Future[Done]] = {
      val writer = maybeOptions.fold(builder)(_.applyTo[T, B](builder)).build()
      sink(writer)
    }
  }

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def rowParquetRecordSink[T: ParquetRecordEncoder: ParquetSchemaResolver](
      file: OutputFile,
      options: ParquetWriter.Options = ParquetWriter.Options()
  ): Sink[T, Future[Done]] = {
    val valueCodecConfiguration = ValueCodecConfiguration(options)
    val schema                  = ParquetSchemaResolver.resolveSchema[T]
    val writer                  = ParquetWriter.internalWriter(file, schema, options)

    def encode(data: T): RowParquetRecord = ParquetRecordEncoder.encode[T](data, valueCodecConfiguration)

    Flow[T]
      .map(encode)
      .toMat(sink(writer))(Keep.right)
  }

  private def sink[T](writer: HadoopParquetWriter[T]): Sink[T, Future[Done]] =
    Flow[T]
      .fold(0) { case (acc, record) => writer.write(record); acc + 1 }
      .map { count =>
        if (logger.isDebugEnabled) logger.debug(s"$count records were successfully written")
        try writer.close()
        catch {
          case _: NullPointerException => // ignores bug in Parquet
        }
      }
      .recover { case NonFatal(e) =>
        Try(writer.close())
        throw e
      }
      .toMat(Sink.ignore)(Keep.right)

}
