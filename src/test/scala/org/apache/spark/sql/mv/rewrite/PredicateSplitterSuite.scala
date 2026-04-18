package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThan, Literal, LessThan}
import org.apache.spark.sql.mv.rewrite.util.PredicateSplitter
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PredicateSplitterSuite extends AnyFunSuite with Matchers {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType)()

  test("split single equality between two attributes") {
    val a = attr("a")
    val b = attr("b")
    val result = PredicateSplitter.split(EqualTo(a, b))
    result.equalities should have size 1
    result.residuals shouldBe empty
    result.equalities.head shouldBe EqualTo(a, b)
  }

  test("split single residual predicate") {
    val a = attr("a")
    val result = PredicateSplitter.split(GreaterThan(a, Literal(10)))
    result.equalities shouldBe empty
    result.residuals should have size 1
  }

  test("split AND predicate with both equality and residual") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val predicate = And(
      EqualTo(a, b),
      GreaterThan(c, Literal(5))
    )
    val result = PredicateSplitter.split(predicate)
    result.equalities should have size 1
    result.residuals should have size 1
  }

  test("split complex AND predicate with multiple equalities and residuals") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val d = attr("d")
    val predicate = And(
      And(EqualTo(a, b), EqualTo(c, d)),
      And(GreaterThan(a, Literal(5)), LessThan(b, Literal(100)))
    )
    val result = PredicateSplitter.split(predicate)
    result.equalities should have size 2
    result.residuals should have size 2
  }

  test("split equality with literal is residual") {
    val a = attr("a")
    val result = PredicateSplitter.split(EqualTo(a, Literal(42)))
    result.equalities shouldBe empty
    result.residuals should have size 1
  }

  test("flatten conjunction") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val predicate = And(And(EqualTo(a, b), GreaterThan(a, Literal(5))), LessThan(c, Literal(10)))
    val conjuncts = PredicateSplitter.flattenConjunction(predicate)
    conjuncts should have size 3
  }

  test("split empty sequence") {
    val result = PredicateSplitter.split(Seq.empty)
    result.equalities shouldBe empty
    result.residuals shouldBe empty
  }
}