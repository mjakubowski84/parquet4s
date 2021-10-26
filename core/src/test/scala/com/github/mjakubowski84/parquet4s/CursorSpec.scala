package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CursorSpec extends AnyFlatSpec with Matchers {

  "Skipping cursor" should "advance until it reaches path to skip" in {
    val (a, b, c, d) = ("a", "b", "c", "d")
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq(Col("a.b.c"))).advanceByFieldName(a)
      cursorAtB <- cursorAtA.advanceByFieldName(b)
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(Col("a.b")))
    cursorAtBOpt.flatMap(_.advanceByFieldName(c)) should be(None)
    cursorAtBOpt.flatMap(_.advanceByFieldName(d)).map(_.path) should be(Some(Col("a.b.d")))
  }

  it should "advance a path that is not related to path to skip" in {
    val (x, y, z) = ("x", "y", "z")
    val pathOpt = for {
      cursorAtX <- Cursor.skipping(Seq(Col("a.b.c"))).advanceByFieldName(x)
      cursorAtY <- cursorAtX.advanceByFieldName(y)
      cursorAtZ <- cursorAtY.advanceByFieldName(z)
    } yield cursorAtZ.path
    pathOpt should be(Some(Col("x.y.z")))
  }

  it should "skip all paths provided in param" in {
    val (a, b, c, d, e) = ("a", "b", "c", "d", "e")
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq(Col("a.b.c"), Col("a.b.d"), Col("a.b.e"))).advanceByFieldName(a)
      cursorAtB <- cursorAtA.advanceByFieldName(b)
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(Col("a.b")))
    cursorAtBOpt.flatMap(_.advanceByFieldName(c)) should be(None)
    cursorAtBOpt.flatMap(_.advanceByFieldName(d)) should be(None)
    cursorAtBOpt.flatMap(_.advanceByFieldName(e)) should be(None)
  }

  it should "skip nothing if no param was provided" in {
    val (a, b, c, d, e) = ("a", "b", "c", "d", "e")
    val paths = for {
      cursorAtA <- Cursor.skipping(Seq.empty).advanceByFieldName(a)
      cursorAtB <- cursorAtA.advanceByFieldName(b)
      cursorAtC <- cursorAtB.advanceByFieldName(c)
      cursorAtD <- cursorAtB.advanceByFieldName(d)
      cursorAtE <- cursorAtB.advanceByFieldName(e)
    } yield (cursorAtC.path, cursorAtD.path, cursorAtE.path)
    paths should be(Some(Col("a.b.c"), Col("a.b.d"), Col("a.b.e")))
  }

  "Following cursor" should "advance a path provided in the parameter and stop when it is reached" in {
    val (a, b, c, d) = ("a", "b", "c", "d")

    val cursorAtCOpt = for {
      cursorAtA <- Cursor.following(Col("a.b.c")).advanceByFieldName(a)
      cursorAtB <- cursorAtA.advanceByFieldName(b)
      cursorAtC <- cursorAtB.advanceByFieldName(c)
    } yield cursorAtC

    cursorAtCOpt.map(_.path) should be(Some(Col("a.b.c")))
    cursorAtCOpt.flatMap(_.advanceByFieldName(d)) should be(None)
  }

  it should "not advance a path that doesn't match the parameter" in {
    val (a, k, x) = ("a", "k", "x")

    Cursor.following(Col("a.b.c")).advanceByFieldName(x) should be(None)
    Cursor.following(Col("a.b.c")).advanceByFieldName(a).flatMap(_.advanceByFieldName(k)) should be(None)
  }

  it should "be completed on start when path is empty" in {
    val a           = "a"
    val emptyCursor = Cursor.following(ColumnPath.Empty)

    emptyCursor.advanceByFieldName(a) should be(None)
    emptyCursor.path should be(empty)
  }

  "Simple cursor" should "advance a path" in {
    val (a, b, c) = ("a", "b", "c")

    val cursorAtCOpt = for {
      cursorAtA <- Cursor.simple.advanceByFieldName(a)
      cursorAtB <- cursorAtA.advanceByFieldName(b)
      cursorAtC <- cursorAtB.advanceByFieldName(c)
    } yield cursorAtC

    cursorAtCOpt.map(_.path) should be(Some(Col("a.b.c")))
  }

}
