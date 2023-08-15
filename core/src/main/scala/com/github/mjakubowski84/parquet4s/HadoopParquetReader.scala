package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.io.InputFile
import org.apache.parquet.schema.MessageType

object HadoopParquetReader {

  private class Builder(
      inputFile: InputFile,
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      metadataReader: MetadataReader
  ) extends org.apache.parquet.hadoop.ParquetReader.Builder[RowParquetRecord](inputFile) {
    override lazy val getReadSupport: ReadSupport[RowParquetRecord] = new ParquetReadSupport(
      projectedSchemaOpt = projectedSchemaOpt,
      columnProjections  = columnProjections,
      metadataReader     = metadataReader
    )
  }

  def apply(
      inputFile: InputFile,
      projectedSchemaOpt: Option[MessageType],
      columnProjections: Seq[ColumnProjection],
      filter: FilterCompat.Filter,
      metadataReader: MetadataReader
  ): org.apache.parquet.hadoop.ParquetReader.Builder[RowParquetRecord] =
    new Builder(inputFile, projectedSchemaOpt, columnProjections, metadataReader).withFilter(filter)

}
