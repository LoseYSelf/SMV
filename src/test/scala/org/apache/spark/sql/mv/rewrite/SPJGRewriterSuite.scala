package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.mv.rewrite.util.ExpressionLineage
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SPJGRewriterSuite extends AnyFunSuite with Matchers {

  test("isSPJGPlan - Filter is SPJG") {
    val relation = LocalRelation(Seq(AttributeReference("a", IntegerType)()))
    val filter = Filter(org.apache.spark.sql.catalyst.expressions.Literal(true), relation)
    ExpressionLineage.isSPJGPlan(filter) shouldBe true
  }

  test("isSPJGPlan - Project is SPJG") {
    val relation = LocalRelation(Seq(AttributeReference("a", IntegerType)()))
    val project = Project(relation.output, relation)
    ExpressionLineage.isSPJGPlan(project) shouldBe true
  }

  test("isSPJGPlan - Aggregate is SPJG") {
    val relation = LocalRelation(Seq(AttributeReference("a", IntegerType)()))
    val agg = Aggregate(Seq(AttributeReference("a", IntegerType)()), relation.output, relation)
    ExpressionLineage.isSPJGPlan(agg) shouldBe true
  }

  test("isSPJGPlan - Sort is not SPJG") {
    val relation = LocalRelation(Seq(AttributeReference("a", IntegerType)()))
    val sort = Sort(Seq.empty, true, relation)
    ExpressionLineage.isSPJGPlan(sort) shouldBe false
  }

  test("isSPJGPlan - Join is SPJG") {
    val left = LocalRelation(Seq(AttributeReference("a", IntegerType)()))
    val right = LocalRelation(Seq(AttributeReference("b", IntegerType)()))
    val join = Join(left, right, Inner, None, JoinHint.NONE)
    ExpressionLineage.isSPJGPlan(join) shouldBe true
  }

  test("isSPJGPlan - complex SPJG with nested operators") {
    val relation = LocalRelation(Seq(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)()
    ))
    val filter = Filter(org.apache.spark.sql.catalyst.expressions.Literal(true), relation)
    val project = Project(relation.output, filter)
    ExpressionLineage.isSPJGPlan(project) shouldBe true
  }
}