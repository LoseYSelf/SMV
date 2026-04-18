package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.mv.rewrite.spjg.MatchModality
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MatchModalitySuite extends AnyFunSuite with Matchers {

  test("COMPLETE - same table sets") {
    val modality = MatchModality.determine(Set("t1", "t2"), Set("t1", "t2"))
    modality shouldBe MatchModality.COMPLETE
  }

  test("QUERY_PARTIAL - view has extra tables") {
    val modality = MatchModality.determine(Set("t1"), Set("t1", "t2"))
    modality shouldBe MatchModality.QUERY_PARTIAL
  }

  test("VIEW_PARTIAL - query has extra tables") {
    val modality = MatchModality.determine(Set("t1", "t2"), Set("t1"))
    modality shouldBe MatchModality.VIEW_PARTIAL
  }

  test("NO_MATCH - no overlapping tables") {
    val modality = MatchModality.determine(Set("t1"), Set("t2"))
    modality shouldBe MatchModality.NO_MATCH
  }

  test("NO_MATCH - partial overlap but neither is subset") {
    val modality = MatchModality.determine(Set("t1", "t2"), Set("t2", "t3"))
    modality shouldBe MatchModality.NO_MATCH
  }

  test("COMPLETE - single table match") {
    val modality = MatchModality.determine(Set("t1"), Set("t1"))
    modality shouldBe MatchModality.COMPLETE
  }

  test("NO_MATCH - empty table sets") {
    val modality = MatchModality.determine(Set.empty, Set("t1"))
    modality shouldBe MatchModality.NO_MATCH
  }
}