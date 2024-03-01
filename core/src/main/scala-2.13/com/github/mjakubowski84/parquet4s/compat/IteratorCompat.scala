package com.github.mjakubowski84.parquet4s.compat

object IteratorCompat {

  @inline
  def unfold[A, S](init: S)(f: S => Option[(A, S)]): Iterator[A] = Iterator.unfold[A, S](init)(f)

}
