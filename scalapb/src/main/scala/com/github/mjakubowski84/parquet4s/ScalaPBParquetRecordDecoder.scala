package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.ScalaPBImplicits.*
import com.google.protobuf.ByteString
import org.apache.parquet.io.api.Binary
import scalapb.descriptors.*
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.util.control.NoStackTrace

case class ScalaPBParquetDecodeException(msg: String) extends RuntimeException(msg) with NoStackTrace

class ScalaPBParquetRecordDecoder[T <: GeneratedMessage: GeneratedMessageCompanion] extends ParquetRecordDecoder[T] {
  private val cmp                                                    = implicitly[GeneratedMessageCompanion[T]]
  private var enumMetadata: collection.Map[String, Map[String, Int]] = Map.empty

  override def setMetadata(metadata: collection.Map[String, String]): Unit =
    enumMetadata = metadata.collect {
      case (key, value) if key.startsWith(MetadataEnumPrefix) =>
        val enumName   = key.substring(MetadataEnumPrefix.length)
        val enumValues = EnumNameNumberPairRe.findAllMatchIn(value).map(m => (m.group(1), m.group(2).toInt)).toMap
        (enumName, enumValues)
    }

  override def decode(record: RowParquetRecord, configuration: ValueCodecConfiguration): T =
    cmp.messageReads.read(decodeMessage(record, cmp.scalaDescriptor.fields))

  private def decodeMessage(row: RowParquetRecord, fields: Vector[FieldDescriptor]): PMessage =
    PMessage(fields.flatMap(fd => row.get(fd.name).map(v => fd -> decodeField(fd, v))).toMap)

  private def decodeField(fd: FieldDescriptor, value: Value): PValue =
    (fd.scalaType, value) match {
      case (ScalaType.Boolean, BooleanValue(value))       => PBoolean(value)
      case (ScalaType.Long, LongValue(value))             => PLong(value)
      case (ScalaType.Float, FloatValue(value))           => PFloat(value)
      case (ScalaType.Double, DoubleValue(value))         => PDouble(value)
      case (ScalaType.Int, IntValue(value))               => PInt(value)
      case (ScalaType.Enum(ed), BinaryValue(value))       => decodeEnum(ed, value)
      case (ScalaType.String, BinaryValue(value))         => PString(value.toStringUsingUTF8)
      case (ScalaType.ByteString, BinaryValue(value))     => PByteString(ByteString.copyFrom(value.toByteBuffer))
      case (_, list: ListParquetRecord)                   => PRepeated(list.iterator.map(decodeField(fd, _)).toVector)
      case (ScalaType.Message(md), row: RowParquetRecord) => decodeMessage(row, md.fields)
      case (ScalaType.Message(md), map: MapParquetRecord) if md.fields.size == 2 => decodeMap(md, map)
      case (_, NullValue) => if (fd.isRepeated) PRepeated(Vector.empty) else PEmpty
      case _ =>
        throw ScalaPBParquetDecodeException(s"Unsupported combination of field and value: ${fd.scalaType}, ${value}")
    }

  private def decodeEnum(ed: EnumDescriptor, value: Binary): PEnum = {
    val name = value.toStringUsingUTF8
    enumMetadata
      .get(ed.fullName)
      .flatMap(_.get(name))
      .map(ed.findValueByNumberCreatingIfUnknown(_))
      .orElse(ed.values.find(_.name == name))
      .map(PEnum(_))
      .getOrElse(throw ScalaPBParquetDecodeException(s"Unrecognized value (${name}) for Enum:${ed.fullName}"))
  }

  private def decodeMap(md: Descriptor, record: MapParquetRecord) = {
    val keyField   = md.fields(0)
    val valueField = md.fields(1)
    PRepeated(
      record.iterator.map { case (key, value) =>
        PMessage(Map(keyField -> decodeField(keyField, key), valueField -> decodeField(valueField, value)))
      }.toVector
    )
  }
}
