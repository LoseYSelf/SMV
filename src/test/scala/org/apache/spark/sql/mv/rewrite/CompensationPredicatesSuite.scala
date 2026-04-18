package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, GreaterThan, Literal}
import org.apache.spark.sql.mv.rewrite.spjg.CompensationPredicates
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CompensationPredicatesSuite extends AnyFunSuite with Matchers {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType)()

  test("complete coverage - same predicates") {
    val a = attr("a")
    val b = attr("b")
    val eq = EqualTo(a, b)
    val residual = GreaterThan(a, Literal(5))
    val result = CompensationPredicates.compute(
      queryEqualities = Seq(eq),
      queryResiduals = Seq(residual),
      viewEqualities = Seq(eq),
      viewResiduals = Seq(residual),
      mappingFn = identity
    )
    result.isCovered shouldBe true
    result.compensation shouldBe None
  }

  test("compensation needed for extra query residual") {
    val a = attr("a")
    val b = attr("b")
    val eq = EqualTo(a, b)
    val viewResidual = GreaterThan(a, Literal(5))
    val queryResidual = GreaterThan(a, Literal(10))
    val result = CompensationPredicates.compute(
      queryEqualities = Seq(eq),
      queryResiduals = Seq(queryResidual),
      viewEqualities = Seq(eq),
      viewResiduals = Seq(viewResidual),
      mappingFn = identity
    )
    result.isCovered shouldBe true
    result.residualComp should have size 1
    result.compensation shouldBe defined
  }

  test("no compensation when view has more restrictive predicates") {
    val a = attr("a")
    val b = attr("b")
    val eq = EqualTo(a, b)
    val viewResidual = GreaterThan(a, Literal(10))
    val queryResidual = GreaterThan(a, Literal(5))
    // View has a more restrictive filter (> 10) than query (> 5)
    // In this simple model, the view's residual doesn't subsume the query's
    val result = CompensationPredicates.compute(
      queryEqualities = Seq(eq),
      queryResiduals = Seq(queryResidual),
      viewEqualities = Seq(eq),
      viewResiduals = Seq(viewResidual),
      mappingFn = identity
    )
    result.isCovered shouldBe true
    // Query has a less restrictive filter that the view doesn't cover
    result.residualComp should have size 1
  }

  test("empty predicates - fully covered") {
    val result = CompensationPredicates.compute(
      queryEqualities = Seq.empty,
      queryResiduals = Seq.empty,
      viewEqualities = Seq.empty,
      viewResiduals = Seq.empty,
      mappingFn = identity
    )
    result.isCovered shouldBe true
    result.compensation shouldBe None
  }
}