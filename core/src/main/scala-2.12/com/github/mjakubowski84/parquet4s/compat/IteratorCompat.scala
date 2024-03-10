package com.github.mjakubowski84.parquet4s.compat

import scala.collection.AbstractIterator

object IteratorCompat {

  private class UnfoldIterator[A, S](init: S, f: S => Option[(A, S)]) extends AbstractIterator[A] {
    private var state: S                  = init
    private var nextValue: Option[(A, S)] = null

    override def hasNext: Boolean = {
      if (nextValue == null) {
        nextValue = f(state)
      }
      nextValue.isDefined
    }

    override def next(): A =
      if (hasNext) {
        val (out, nextState) = nextValue.get
        state     = nextState
        nextValue = null
        out
      } else {
        Iterator.empty.next()
      }

  }

  @inline
  def unfold[A, S](init: S)(f: S => Option[(A, S)]): Iterator[A] = new UnfoldIterator[A, S](init, f)
}
