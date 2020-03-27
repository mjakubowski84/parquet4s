package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader => HadoopParquetReader}
import org.slf4j.{Logger, LoggerFactory}

private[parquet4s] object ParquetSource extends IOOps {

  override protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T: ParquetRecordDecoder](path: Path,
                                     options: ParquetReader.Options,
                                     filter: Filter
                                    ): Source[T, NotUsed] = {
    val valueCodecConfiguration = options.toValueCodecConfiguration
    val hadoopConf = options.hadoopConf

    findPartitionedPaths(path, hadoopConf).fold(
      Source.failed,
      partitionedPaths => {

        partitionedPaths.map { partitionedPath =>
          val builder = HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), partitionedPath.path)
            .withConf(hadoopConf)
            .withFilter(filter.toFilterCompat(valueCodecConfiguration))

          def decode(record: RowParquetRecord): T = ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)

          Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
            create = builder.build,
            read = reader => Option(reader.read()),
            close = _.close()
          ).map { record =>
            partitionedPath.partitionMap.foreach { case (name, value) =>
              record.add(name, BinaryValue(value))
            }
            record
          }.map(decode)
        }.reduceLeft(_.concat(_))

      }
    )
  }

}
