package com.github.mjakubowski84.parquet4s

object MapCompat {
  inline def remove[K, V](map: Map[K, V], key: K): Map[K, V] = map.removed(key)
}

trait MapCompat {

  this: MapParquetRecord =>

  /** Removes a single entry from this map.
   *
   * @param key key of the element to remove
   * @return map of inner entries with the element removed
   */
  override def removed(key: Value): MapParquetRecord =
    new MapParquetRecord(MapCompat.remove(entries, key))

  /** Adds a single entry to this map.
   *
   *  @param key key of the entry to add
   *  @param value value of the entry to add
   *  @return map of inner entries updated with the new entry added
   */
  override def updated[V1 >: Value](key: Value, value: V1): Map[Value, V1] =
    entries.updated(key, value)

}
