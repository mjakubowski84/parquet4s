package com.github.mjakubowski84.parquet4s

object MapCompat {
  @inline
  def remove[K, V](map: Map[K, V], key: K): Map[K, V] = map - key
}

trait MapCompat {

  this: MapParquetRecord =>

  /** Removes a single entry from this map.
    *
    * @param key
    *   the key of the entry to remove.
    * @return
    *   the [[MapParquetRecord]] itself
    */
  override def -(key: Value): MapParquetRecord =
    new MapParquetRecord(MapCompat.remove(entries, key))

  /** Adds a single entry to this map.
    *
    * @param entry
    *   the element to add
    * @return
    *   map of inner values entries with the entry added
    */
  override def +[V1 >: Value](entry: (Value, V1)): Map[Value, V1] =
    entries + entry

}
