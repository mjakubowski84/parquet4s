package com.github.mjakubowski84.parquet4s

import org.apache.parquet.filter2.predicate.{Statistics, UserDefinedPredicate}

/** Extend this trait in order to build a non-standard filter. <br> <b> Please note! </b> When defining <b>V</b>, use Java
  * types supported by Parquet such as:
  *   1. [[java.lang.Boolean]]
  *   1. [[java.lang.Int]]
  *   1. [[java.lang.Long]]
  *   1. [[java.lang.Double]]
  *   1. [[java.lang.Float]]
  *   1. [[org.apache.parquet.io.api.Binary]]
  *
  * @tparam V
  *   Type of column custom filter refers to
  */
trait UDP[V] {

  /** Used to filter record by record.
    *
    * @param value
    *   column value of record that is filtered
    * @return
    *   `true` if record containing given value should be kept
    */
  def keep(value: V): Boolean

  /** Used to drop a whole row group if collected statistics do not match your requirements.
    *
    * @param statistics
    *   of the row group
    * @return
    *   `true` if the whole row group can be omitted
    */
  def canDrop(statistics: FilterStatistics[V]): Boolean

  /** It is an opposite of `canDrop`. There is a separate function for inverse comparison as the some types may require
    * quite a different logic for that. This function will be called when processing `not` predicates.
    *
    * @param statistics
    *   of the row group
    * @return
    *   `true` if the whole row group can be omitted for inverse filter
    */
  def inverseCanDrop(statistics: FilterStatistics[V]): Boolean

  /** @return
    *   name of the filter
    */
  def name: String
}

/** Row group statistics then can be used in [[UDP]] to drop unwanted data.
  * @param min
  *   minimum value of `V` in a row group
  * @param max
  *   maximum value of `V` in a row group
  * @param ordering
  *   [[scala.Ordering]] of `V`
  * @tparam V
  *   user type of column
  */
class FilterStatistics[V](val min: V, val max: V)(implicit val ordering: Ordering[V])

private[parquet4s] class UDPAdapter[V <: Comparable[V]](udp: UDP[V])(implicit ordering: Ordering[V])
    extends UserDefinedPredicate[V]
    with Serializable {

  override def keep(value: V): Boolean = udp.keep(value)

  override def canDrop(statistics: Statistics[V]): Boolean =
    udp.canDrop(convert(statistics))

  override def inverseCanDrop(statistics: Statistics[V]): Boolean =
    udp.inverseCanDrop(convert(statistics))

  override def toString: String = udp.name

  private def convert(statistics: Statistics[V]) = new FilterStatistics[V](statistics.getMin, statistics.getMax)

}
