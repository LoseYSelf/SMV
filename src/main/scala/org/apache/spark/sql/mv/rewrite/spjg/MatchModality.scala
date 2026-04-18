package org.apache.spark.sql.mv.rewrite.spjg

/**
 * Represents the match modality between a query and a materialized view,
 * based on how their table sets relate to each other.
 *
 * This classification determines what compensation is needed for rewriting.
 * Refer to Calcite's MaterializedViewRule for the original classification.
 */
sealed trait MatchModality

object MatchModality {

  /**
   * COMPLETE: The query and the MV reference exactly the same set of tables.
   * No extra tables on either side. Only residual predicate compensation may be needed.
   */
  case object COMPLETE extends MatchModality

  /**
   * QUERY_PARTIAL: The MV references a superset of the query's tables.
   * The MV has extra tables not present in the query.
   * Requires: The extra joins in the MV must be cardinality-preserving
   * (e.g., UK-FK joins that don't introduce duplicate rows).
   * Compensation: Project away the extra columns from the MV.
   */
  case object QUERY_PARTIAL extends MatchModality

  /**
   * VIEW_PARTIAL: The query references a superset of the MV's tables.
   * The query has extra tables not present in the MV.
   * Requires: The extra tables in the query can be joined above the MV.
   * Compensation: Add additional joins on top of the MV scan.
   */
  case object VIEW_PARTIAL extends MatchModality

  /**
   * NO_MATCH: The query and MV have no overlapping tables, or their
   * table sets are incompatible for any form of rewriting.
   */
  case object NO_MATCH extends MatchModality

  /**
   * Determine the match modality between a query's tables and an MV's tables.
   *
   * @param queryTables Set of table identifiers in the query
   * @param viewTables  Set of table identifiers in the MV definition
   * @return The appropriate MatchModality
   */
  def determine(queryTables: Set[String], viewTables: Set[String]): MatchModality = {
    if (queryTables == viewTables) {
      COMPLETE
    } else if (viewTables.subsetOf(queryTables) && viewTables.nonEmpty) {
      VIEW_PARTIAL
    } else if (queryTables.subsetOf(viewTables) && queryTables.nonEmpty) {
      QUERY_PARTIAL
    } else {
      NO_MATCH
    }
  }
}