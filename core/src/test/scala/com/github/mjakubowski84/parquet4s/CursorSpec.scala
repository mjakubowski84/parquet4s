package com.github.mjakubowski84.parquet4s

import com.github.mjakubowski84.parquet4s.Cursor.DotPath
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CursorSpec extends AnyFlatSpec with Matchers {

  "Skipping cursor" should "advance until it reaches path to skip" in {
    val (a, b, c, d) = ('a, 'b, 'c, 'd)
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq("a.b.c")).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(DotPath("a.b")))
    cursorAtBOpt.flatMap(_.advance[c.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[d.type]).map(_.path) should be(Some(DotPath("a.b.d")))
  }

  it should "advance a path that is not related to path to skip" in {
    val (x, y, z) = ('x, 'y, 'z)
    val pathOpt = for {
      cursorAtX <- Cursor.skipping(Seq("a.b.c")).advance[x.type]
      cursorAtY <- cursorAtX.advance[y.type]
      cursorAtZ <- cursorAtY.advance[z.type]
    } yield cursorAtZ.path
    pathOpt should be(Some(DotPath("x.y.z")))
  }

  it should "skip all paths provided in param" in {
    val (a, b, c, d, e) = ('a, 'b, 'c, 'd, 'e)
    val cursorAtBOpt = for {
      cursorAtA <- Cursor.skipping(Seq("a.b.c", "a.b.d", "a.b.e")).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
    } yield cursorAtB

    cursorAtBOpt.map(_.path) should be(Some(DotPath("a.b")))
    cursorAtBOpt.flatMap(_.advance[c.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[d.type]) should be(None)
    cursorAtBOpt.flatMap(_.advance[e.type]) should be(None)
  }

  it should "skip nothing if no param was provided" in {
    val (a, b, c, d, e) = ('a, 'b, 'c, 'd, 'e)
    val paths = for {
      cursorAtA <- Cursor.skipping(Seq.empty).advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
      cursorAtC <- cursorAtB.advance[c.type]
      cursorAtD <- cursorAtB.advance[d.type]
      cursorAtE <- cursorAtB.advance[e.type]
    } yield (cursorAtC.path, cursorAtD.path, cursorAtE.path)
    paths should be(Some(DotPath("a.b.c"), DotPath("a.b.d"), DotPath("a.b.e")))
  }

  "Following cursor" should "advance a path provided in the parameter and stop when it is reached" in {
    val (a, b, c, d) = ('a, 'b, 'c, 'd)

    val cursorAtCOpt = for {
      cursorAtA <- Cursor.following("a.b.c").advance[a.type]
      cursorAtB <- cursorAtA.advance[b.type]
      cursorAtC <- cursorAtB.advance[c.type]
    } yield cursorAtC

    cursorAtCOpt.map(_.path) should be(Some(DotPath("a.b.c")))
    cursorAtCOpt.map(_.objective) should be(Some("a.b.c"))
    cursorAtCOpt.flatMap(_.advance[d.type]) should be(None)
  }

  it should "not advance a path that doesn't match the parameter" in {
    val (a, k, x) = ('a, 'k, 'x)

    Cursor.following("a.b.c").advance[x.type] should be(None)
    Cursor.following("a.b.c").advance[a.type].flatMap(_.advance[k.type]) should be(None)
  }

  it should "be completed on start when path is empty" in {
    val a = 'a
    val emptyCursor = Cursor.following("")

    emptyCursor.advance[a.type] should be(None)
    emptyCursor.objective should be("")
    emptyCursor.path should be(empty)
  }

}
