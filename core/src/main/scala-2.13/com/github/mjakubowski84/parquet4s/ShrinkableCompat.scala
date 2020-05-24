package com.github.mjakubowski84.parquet4s

trait ShrinkableCompat {

  this: MapParquetRecord =>

  /** Removes a single entry from this map.
   *
   *  @param key  the key of the entry to remove.
   *  @return the MapParquetRecord itself
   */
  override def subtractOne(key: Value): This = {
    entries -= key
    this
  }

  /** ${Add}s a single element to this map.
   *
   *  @param elem  the element to $add.
   *  @return the MapParquetRecord itself
   */
  override def addOne(elem: (Value, Value)): This = {
    entries += elem
    this
  }

}
