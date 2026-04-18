package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.mv.rewrite.spjg.AggregateRollup
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AggregateRollupSuite extends AnyFunSuite with Matchers {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType)()

  test("isGroupingSubset - exact match") {
    val a = attr("a")
    val b = attr("b")
    AggregateRollup.isGroupingSubset(Seq(a, b), Seq(a, b)) shouldBe true
  }

  test("isGroupingSubset - proper subset") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    AggregateRollup.isGroupingSubset(Seq(a), Seq(a, b, c)) shouldBe true
  }

  test("isGroupingSubset - not a subset") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val d = attr("d")
    AggregateRollup.isGroupingSubset(Seq(a, d), Seq(a, b, c)) shouldBe false
  }

  test("isGroupingSubset - empty is subset of anything") {
    val a = attr("a")
    AggregateRollup.isGroupingSubset(Seq.empty, Seq(a)) shouldBe true
  }

  test("isGroupingSubset - non-empty is not subset of empty") {
    val a = attr("a")
    AggregateRollup.isGroupingSubset(Seq(a), Seq.empty) shouldBe false
  }
}