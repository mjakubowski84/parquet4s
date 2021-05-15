package com.github.mjakubowski84.parquet4s

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CursorSpec extends AnyFlatSpec with Matchers {

  "Skipping cursor" should "advance until it reaches path to skip" in {
    val (a, b, c, d) = (Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"))
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq(Col("a.b.c"))).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(Col("a.b")))
    cursorAtBOpt.flatMap(_.advance[c.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[d.type]).map(_.path) should be(Some(Col("a.b.d")))
  }

  it should "advance a path that is not related to path to skip" in {
    val (x, y, z) = (Symbol("x"), Symbol("y"), Symbol("z"))
    val pathOpt = for {
      cursorAtX <- Cursor.skipping(Seq(Col("a.b.c"))).advance[x.type]
      cursorAtY <- cursorAtX.advance[y.type]
      cursorAtZ <- cursorAtY.advance[z.type]
    } yield cursorAtZ.path
    pathOpt should be(Some(Col("x.y.z")))
  }

  it should "skip all paths provided in param" in {
    val (a, b, c, d, e) = (Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"), Symbol("e"))
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq(Col("a.b.c"), Col("a.b.d"), Col("a.b.e"))).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(Col("a.b")))
    cursorAtBOpt.flatMap(_.advance[c.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[d.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[e.type]) should be(None)
  }

  it should "skip nothing if no param was provided" in {
    val (a, b, c, d, e) = (Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"), Symbol("e"))
    val paths = for {
      cursorAtA <- Cursor.skipping(Seq.empty).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
      cursorAtC <- cursorAtB.advance[c.type]
      cursorAtD <- cursorAtB.advance[d.type]
      cursorAtE <- cursorAtB.advance[e.type]
    } yield (cursorAtC.path, cursorAtD.path, cursorAtE.path)
    paths should be(Some(Col("a.b.c"), Col("a.b.d"), Col("a.b.e")))
  }

  "Following cursor" should "advance a path provided in the parameter and stop when it is reached" in {
    val (a, b, c, d) = (Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"))

    val cursorAtCOpt = for {
      cursorAtA <- Cursor.following(Col("a.b.c")).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
      cursorAtC <- cursorAtB.advance[c.type]
    } yield cursorAtC

    cursorAtCOpt.map(_.path) should be(Some(Col("a.b.c")))
    cursorAtCOpt.flatMap(_.advance[d.type]) should be(None)
  }

  it should "not advance a path that doesn't match the parameter" in {
    val (a, k, x) = (Symbol("a"), Symbol("k"), Symbol("x"))

    Cursor.following(Col("a.b.c")).advance[x.type] should be(None)
    Cursor.following(Col("a.b.c")).advance[a.type].flatMap(_.advance[k.type]) should be(None)
  }

  it should "be completed on start when path is empty" in {
    val a = Symbol("a")
    val emptyCursor = Cursor.following(ColumnPath.Empty)

    emptyCursor.advance[a.type] should be(None)
    emptyCursor.path should be(empty)
  }

  "Simple cursor" should "advance a path" in {
    val (a, b, c) = (Symbol("a"), Symbol("b"), Symbol("c"))

    val cursorAtCOpt = for {
      cursorAtA <- Cursor.simple.advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
      cursorAtC <- cursorAtB.advance[c.type]
    } yield cursorAtC

    cursorAtCOpt.map(_.path) should be(Some(Col("a.b.c")))
  }

}
