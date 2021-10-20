package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.predicate.{Statistics, UserDefinedPredicate}

/** Extend this trait in order to build your custom filter.
  *
  * @tparam In
  *   Type of column custom filter refers to
  */
trait UDP[In] {

  /** Used to filter record by record.
    *
    * @param value
    *   column value of record that is filtered
    * @return
    *   `true` if record containing given value should be kept
    */
  def keep(value: In): Boolean

  /** Used to drop a whole row group if collected statistics do not match your requirements.
    *
    * @param statistics
    *   of the row group
    * @return
    *   `true` if the whole row group can be omitted
    */
  def canDrop(statistics: FilterStatistics[In]): Boolean

  /** It is an opposite of `canDrop`. There is a separate function for inverse comparison as the some types may require
    * quite a different logic for that. This function will be called when processing `not` predicates.
    *
    * @param statistics
    *   of the row group
    * @return
    *   `true` if the whole row group can be omitted for inverse filter
    */
  def inverseCanDrop(statistics: FilterStatistics[In]): Boolean

  /** @return
    *   name of the filter
    */
  def name: String
}

/** Row group statistics then can be used in [[UDP]] to drop unwanted data.
  * @param min
  *   minimum value of `T` in a row group
  * @param max
  *   maximum value of `T` in a row group
  * @param ordering
  *   [[scala.Ordering]] of `T`
  * @tparam T
  *   user type of column
  */
class FilterStatistics[T](val min: T, val max: T)(implicit val ordering: Ordering[T])

private[parquet4s] class UDPAdapter[In, V <: Comparable[V]](
    udp: UDP[In],
    decoder: FilterDecoder[In, V],
    valueCodecConfiguration: ValueCodecConfiguration
)(implicit ordering: Ordering[In])
    extends UserDefinedPredicate[V]
    with Serializable {

  override def keep(value: V): Boolean = udp.keep(decoder.decode(value, valueCodecConfiguration))

  override def canDrop(statistics: Statistics[V]): Boolean =
    udp.canDrop(convert(statistics))

  override def inverseCanDrop(statistics: Statistics[V]): Boolean =
    udp.inverseCanDrop(convert(statistics))

  override def toString: String = udp.name

  private def convert(statistics: Statistics[V]) = new FilterStatistics[In](
    decoder.decode(statistics.getMin, valueCodecConfiguration),
    decoder.decode(statistics.getMax, valueCodecConfiguration)
  )
}
