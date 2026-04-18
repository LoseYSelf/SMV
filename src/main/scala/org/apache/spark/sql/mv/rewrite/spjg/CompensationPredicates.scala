package org.apache.spark.sql.mv.rewrite.spjg

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.mv.rewrite.util.PredicateSplitter

/**
 * Computes compensation predicates for materialized view rewriting.
 *
 * Compensation predicates are the residual conditions that must be applied
 * on top of the materialized view scan to produce the correct query result.
 *
 * There are two types of compensation:
 * 1. Equality compensation: When the query's equivalence classes are not
 *    fully covered by the MV's equivalence classes (after mapping).
 * 2. Residual compensation: When the query has predicates not implied
 *    by the MV's predicates (after mapping).
 */
object CompensationPredicates {

  /**
   * Result of compensation predicate computation.
   *
   * @param isCovered      Whether the MV can cover this query at all
   * @param compensation   Predicates that need to be applied on top of the MV scan
   * @param equalityComp   Equality-based compensation predicates
   * @param residualComp   Residual compensation predicates
   */
  case class CompensationResult(
      isCovered: Boolean,
      compensation: Option[Expression],
      equalityComp: Seq[Expression],
      residualComp: Seq[Expression]
  )

  /**
   * Compute compensation predicates for rewriting a query using a materialized view.
   *
   * @param queryEqualities  Equality predicates from the query
   * @param queryResiduals   Residual predicates from the query
   * @param viewEqualities   Equality predicates from the MV definition
   * @param viewResiduals    Residual predicates from the MV definition
   * @param mappingFn        Function to map view expressions to query namespace
   * @return CompensationResult indicating coverage and required compensation
   */
  def compute(
      queryEqualities: Seq[Expression],
      queryResiduals: Seq[Expression],
      viewEqualities: Seq[Expression],
      viewResiduals: Seq[Expression],
      mappingFn: Expression => Expression): CompensationResult = {

    // Map view predicates to query namespace
    val mappedViewEqualities = viewEqualities.map(mappingFn)
    val mappedViewResiduals = viewResiduals.map(mappingFn)

    // Build equivalence classes for query and mapped view
    val queryEqClasses = EquivalenceClasses()
    queryEqualities.foreach {
      case eq: org.apache.spark.sql.catalyst.expressions.EqualTo =>
        queryEqClasses.addEquality(eq)
      case _ =>
    }

    val viewEqClasses = EquivalenceClasses()
    mappedViewEqualities.foreach {
      case eq: org.apache.spark.sql.catalyst.expressions.EqualTo =>
        viewEqClasses.addEquality(eq)
      case _ =>
    }

    // Check if query equivalence classes are covered by view equivalence classes
    val eqCovered = queryEqClasses.isCoveredBy(viewEqClasses)

    // Compute residual compensation: query residuals not implied by view residuals
    val residualComp = computeResidualCompensation(queryResiduals, mappedViewResiduals)

    // Determine overall coverage
    val isCovered = eqCovered

    // Combine compensation predicates
    val allCompensation = (if (!eqCovered) queryEqualities else Seq.empty) ++ residualComp
    val combinedCompensation = if (allCompensation.nonEmpty) {
      Some(allCompensation.reduce(And))
    } else {
      None
    }

    CompensationResult(
      isCovered = isCovered,
      compensation = combinedCompensation,
      equalityComp = if (!eqCovered) queryEqualities else Seq.empty,
      residualComp = residualComp
    )
  }

  /**
   * Compute residual compensation predicates.
   * A query residual needs compensation if it is not implied by any view residual.
   *
   * Simple implementation: a query residual needs compensation if there is no
   * semantically equivalent view residual. Uses expression equality for now;
   * a more sophisticated implementation would use expression equivalence.
   */
  private def computeResidualCompensation(
      queryResiduals: Seq[Expression],
      mappedViewResiduals: Seq[Expression]): Seq[Expression] = {
    queryResiduals.filterNot { qResidual =>
      mappedViewResiduals.exists { vResidual =>
        qResidual.semanticEquals(vResidual)
      }
    }
  }
}