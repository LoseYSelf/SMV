package org.apache.spark.sql.mv.rewrite.util

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression}

object PredicateSplitter {

  case class SplitResult(
      equalities: Seq[EqualTo],
      residuals: Seq[Expression]
  )

  def split(predicate: Expression): SplitResult = {
    val conjuncts = flattenConjunction(predicate)
    val equalities = scala.collection.mutable.ArrayBuffer[EqualTo]()
    val residuals = scala.collection.mutable.ArrayBuffer[Expression]()

    conjuncts.foreach { expr =>
      expr match {
        case eq @ EqualTo(left: AttributeReference, right: AttributeReference) =>
          equalities += eq
        case _ =>
          residuals += expr
      }
    }

    SplitResult(equalities.toSeq, residuals.toSeq)
  }

  def split(conjuncts: Seq[Expression]): SplitResult = {
    if (conjuncts.isEmpty) {
      SplitResult(Seq.empty, Seq.empty)
    } else {
      split(conjuncts.reduce(And))
    }
  }

  def flattenConjunction(expression: Expression): Seq[Expression] = {
    expression match {
      case And(left, right) =>
        flattenConjunction(left) ++ flattenConjunction(right)
      case other => Seq(other)
    }
  }
}