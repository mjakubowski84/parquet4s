package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ParquetRecordEncoder.EncodingException
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

import scala.util.control.NonFatal

/** Type class that allows to encode given data entity as [[RowParquetRecord]]. Can skip some fields according to
  * indication of the [[Cursor]].
  * @tparam T
  *   type of source data
  */
trait SkippingParquetRecordEncoder[T] {

  /** @param cursor
    *   cursor that facilitates traversal over T
    * @param entity
    *   data to be encoded
    * @param configuration
    *   [ValueCodecConfiguration] used by some codecs
    * @return
    *   [[RowParquetRecord]] containing product elements from the data
    */
  def encode(cursor: Cursor, entity: T, configuration: ValueCodecConfiguration): RowParquetRecord

}

object SkippingParquetRecordEncoder {

  def encode[T](
      toSkip: Iterable[String],
      entity: T,
      configuration: ValueCodecConfiguration = ValueCodecConfiguration.default
  )(implicit ev: SkippingParquetRecordEncoder[T]): RowParquetRecord =
    ev.encode(Cursor.skipping(toSkip), entity, configuration)

  implicit val hnilEncoder: SkippingParquetRecordEncoder[HNil] = new SkippingParquetRecordEncoder[HNil] {
    override def encode(cursor: Cursor, nil: HNil, configuration: ValueCodecConfiguration): RowParquetRecord =
      RowParquetRecord.empty
  }

  implicit def hconsEncoder[FieldName <: Symbol, Head, Tail <: HList](implicit
      witness: Witness.Aux[FieldName],
      valueCodec: ValueCodec[Head],
      headEncoder: SkippingValueEncoder[Head] = defaultSkippingValueEncoder[Head],
      tailEncoder: SkippingParquetRecordEncoder[Tail]
  ): SkippingParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] =
    new SkippingParquetRecordEncoder[FieldType[FieldName, Head] :: Tail] {
      override def encode(
          cursor: Cursor,
          entity: FieldType[FieldName, Head] :: Tail,
          configuration: ValueCodecConfiguration
      ): RowParquetRecord =
        cursor.advance[FieldName] match {
          case Some(newCursor) =>
            val fieldName = witness.value.name
            val fieldValueOpt =
              try headEncoder.encode(newCursor, entity.head, valueCodec, configuration)
              catch {
                case NonFatal(cause) =>
                  throw EncodingException(
                    s"Failed to encode field $fieldName: ${entity.head}, due to ${cause.getMessage}",
                    cause
                  )
              }
            fieldValueOpt.foldLeft(tailEncoder.encode(cursor, entity.tail, configuration))(_.prepend(fieldName, _))
          case None =>
            tailEncoder.encode(cursor, entity.tail, configuration)
        }
    }

  implicit def genericEncoder[A, R](implicit
      gen: LabelledGeneric.Aux[A, R],
      encoder: Lazy[SkippingParquetRecordEncoder[R]]
  ): SkippingParquetRecordEncoder[A] =
    new SkippingParquetRecordEncoder[A] {
      override def encode(cursor: Cursor, entity: A, configuration: ValueCodecConfiguration): RowParquetRecord =
        encoder.value.encode(cursor, gen.to(entity), configuration)
    }

  trait SkippingValueEncoder[T] {

    def encode(
        cursor: Cursor,
        data: T,
        valueCodec: ValueCodec[T],
        configuration: ValueCodecConfiguration
    ): Option[Value]

  }

  def defaultSkippingValueEncoder[T]: SkippingValueEncoder[T] = new SkippingValueEncoder[T] {
    override def encode(
        cursor: Cursor,
        data: T,
        valueCodec: ValueCodec[T],
        configuration: ValueCodecConfiguration
    ): Option[Value] =
      Option(valueCodec.encode(data, configuration))
  }

  implicit def productSkippingValueEncoder[T](implicit
      skippingEncoder: SkippingParquetRecordEncoder[T]
  ): SkippingValueEncoder[T] =
    new SkippingValueEncoder[T] {
      override def encode(
          cursor: Cursor,
          data: T,
          valueCodec: ValueCodec[T],
          configuration: ValueCodecConfiguration
      ): Option[Value] = {
        val record = skippingEncoder.encode(cursor, data, configuration)
        if (record.isEmpty) None
        else Some(record)
      }
    }

}
