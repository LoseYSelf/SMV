package org.apache.spark.sql.mv.rewrite.spjg

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.mv.model.MVDefinition
import org.apache.spark.sql.mv.rewrite.util.{ExpressionLineage, PredicateSplitter, SPJGComponents}

/**
 * Core SPJG rewriter that attempts to rewrite a query plan using a
 * materialized view definition.
 *
 * Algorithm (adapted from Calcite's MaterializedViewRule):
 * 1. Validate that the query plan is SPJG-only
 * 2. Extract query metadata (tables, predicates, equivalence classes)
 * 3. Extract MV metadata
 * 4. Determine match modality (COMPLETE/QUERY_PARTIAL/VIEW_PARTIAL)
 * 5. Generate table mapping
 * 6. Compute compensation predicates
 * 7. Build rewritten plan with MVRelation as the scan source
 */
object SPJGRewriter {

  /**
   * Result of an SPJG rewriting attempt.
   */
  case class RewriteResult(
      success: Boolean,
      rewrittenPlan: Option[LogicalPlan],
      reason: Option[String]
  )

  /**
   * Attempt to rewrite a query plan using the given materialized view definition.
   *
   * @param queryPlan  The original query logical plan
   * @param mvDef      The materialized view definition
   * @return RewriteResult indicating success/failure and the rewritten plan
   */
  def rewrite(queryPlan: LogicalPlan, mvDef: MVDefinition): RewriteResult = {
    // Step 1: Validate query is SPJG
    if (!ExpressionLineage.isSPJGPlan(queryPlan)) {
      return RewriteResult(
        success = false,
        None,
        Some("Query plan contains non-SPJG operators")
      )
    }

    // Step 2: Extract query components
    val queryComponents = ExpressionLineage.extractSPJGComponents(queryPlan)
    if (queryComponents.isEmpty) {
      return RewriteResult(
        success = false,
        None,
        Some("Failed to extract SPJG components from query")
      )
    }
    val qc = queryComponents.get

    // Step 3: Extract MV components
    if (!ExpressionLineage.isSPJGPlan(mvDef.queryPlan)) {
      return RewriteResult(
        success = false,
        None,
        Some("MV definition contains non-SPJG operators")
      )
    }
    val mvComponents = ExpressionLineage.extractSPJGComponents(mvDef.queryPlan)
    if (mvComponents.isEmpty) {
      return RewriteResult(
        success = false,
        None,
        Some("Failed to extract SPJG components from MV definition")
      )
    }
    val mc = mvComponents.get

    // Step 4: Determine match modality
    val queryTableNames = qc.tables.keySet
    val viewTableNames = mc.tables.keySet
    val modality = MatchModality.determine(queryTableNames, viewTableNames)

    if (modality == MatchModality.NO_MATCH) {
      return RewriteResult(
        success = false,
        None,
        Some(s"Table sets do not match: query=$queryTableNames, view=$viewTableNames")
      )
    }

    // Step 5: Generate table mapping
    val mappings = TableMappingGenerator.generate(
      queryPlan, mvDef.queryPlan, qc.tables, mc.tables
    )
    if (mappings.isEmpty) {
      return RewriteResult(
        success = false,
        None,
        Some("Could not generate table mapping between query and MV")
      )
    }
    val mapping = mappings.head

    // Step 6: Split predicates and compute compensation
    val querySplit = PredicateSplitter.split(
      PredicateSplitter.flattenConjunction(qc.filters.reduceOption(And).getOrElse(Literal(true)))
    )
    val viewSplit = PredicateSplitter.split(
      PredicateSplitter.flattenConjunction(mc.filters.reduceOption(And).getOrElse(Literal(true)))
    )

    // Map view predicates to query namespace for compensation computation
    val reverseMappingFn = (expr: Expression) =>
      TableMappingGenerator.translateExpressionReverse(expr, mapping)

    val compensation = CompensationPredicates.compute(
      querySplit.equalities,
      querySplit.residuals,
      viewSplit.equalities,
      viewSplit.residuals,
      reverseMappingFn
    )

    if (!compensation.isCovered) {
      return RewriteResult(
        success = false,
        None,
        Some("Query equivalence classes not covered by MV equivalence classes")
      )
    }

    // Step 7: Build rewritten plan
    val rewrittenPlan = buildRewrittenPlan(queryPlan, mvDef, qc, mc, compensation, mapping, modality)
    RewriteResult(success = true, Some(rewrittenPlan), None)
  }

  /**
   * Build the final rewritten logical plan.
   */
  private def buildRewrittenPlan(
      queryPlan: LogicalPlan,
      mvDef: MVDefinition,
      queryComponents: SPJGComponents,
      mvComponents: SPJGComponents,
      compensation: CompensationPredicates.CompensationResult,
      mapping: TableMapping,
      modality: MatchModality): LogicalPlan = {

    // Start with MVRelation as the base scan
    import org.apache.spark.sql.mv.plan.MVRelation
    var plan: LogicalPlan = MVRelation(mvDef)

    // Handle aggregate rollup if needed
    (queryComponents.aggregate, mvComponents.aggregate) match {
      case (Some(queryAgg), Some(mvAgg)) =>
        val rollup = AggregateRollup.computeRollup(
          queryAgg, mvAgg, mvDef.outputSchema
        )
        if (rollup.canRollup) {
          if (queryComponents.aggregate.isDefined &&
              queryComponents.aggregate.get.groupingExpressions.length <
              mvComponents.aggregate.get.groupingExpressions.length) {
            // Need to re-aggregate with fewer grouping columns
            plan = Aggregate(
              queryComponents.aggregate.get.groupingExpressions,
              rollup.rollupExprs,
              plan
            )
          }
        }
      case (Some(_), None) =>
        // Query has aggregation but MV doesn't — can't rewrite this way
        return queryPlan
      case (None, Some(_)) =>
        // MV has aggregation but query doesn't — just use MV data
      case (None, None) =>
        // No aggregation on either side — direct substitution
    }

    // Apply compensation predicates as filters
    plan = compensation.compensation match {
      case Some(cond) => Filter(cond, plan)
      case None => plan
    }

    // Handle VIEW_PARTIAL: add extra joins for tables not in the MV
    if (modality == MatchModality.VIEW_PARTIAL) {
      val extraTables = queryComponents.tables.keySet -- mvComponents.tables.keySet
      extraTables.foreach { tableName =>
        queryComponents.tables.get(tableName).foreach { tablePlan =>
          plan = Join(plan, tablePlan, Inner, None, JoinHint.NONE)
        }
      }
    }

    // Apply final projection to match query output
    val queryOutput = queryPlan.output
    plan = Project(queryOutput, plan)

    plan
  }
}