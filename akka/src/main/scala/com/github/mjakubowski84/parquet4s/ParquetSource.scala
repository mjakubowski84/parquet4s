package com.github.mjakubowski84.parquet4s

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.mjakubowski84.parquet4s.FilterRewriter.{IsFalse, IsTrue}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterPredicate
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
      partitionedDirectory => {

        PartitionFilter.filter(filter.toPredicate(valueCodecConfiguration), partitionedDirectory)
          .map { case (filterCompat, partitionedPath) =>

            def decode(record: RowParquetRecord): T = ParquetRecordDecoder.decode[T](record, valueCodecConfiguration)

            Source.unfoldResource[RowParquetRecord, HadoopParquetReader[RowParquetRecord]](
              create = () => createReader(hadoopConf, filterCompat, partitionedPath),
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

  private def createReader(hadoopConf: Configuration,
                           filterCompat: FilterCompat.Filter,
                           partitionedPath: PartitionedPath): HadoopParquetReader[RowParquetRecord] =
    HadoopParquetReader.builder[RowParquetRecord](new ParquetReadSupport(), partitionedPath.path)
      .withConf(hadoopConf)
      .withFilter(filterCompat)
      .build()

}
