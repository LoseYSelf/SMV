package org.apache.spark.sql.mv.rewrite.spjg

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Generates a mapping between query tables and materialized view tables.
 *
 * This mapping is essential for:
 * 1. Determining which query expressions correspond to which MV expressions
 * 2. Translating equivalence classes between query and MV namespaces
 * 3. Handling self-join scenarios where the same table appears multiple times
 *
 * A TableMapping maps each query table to a corresponding MV table
 * with an attribute-level mapping.
 */
case class TableMapping(
    queryToViewTable: Map[String, String],
    queryToViewAttr: Map[Attribute, Attribute],
    viewToQueryAttr: Map[Attribute, Attribute]
)

/**
 * Generates table mappings between a query and a materialized view.
 */
object TableMappingGenerator {

  /**
   * Generate a mapping from query tables/attributes to view tables/attributes.
   *
   * For each table that appears in both the query and the view, we try to
   * find a bijection between their attributes based on name matching.
   *
   * @param queryPlan The query logical plan
   * @param viewPlan  The materialized view's defining query plan
   * @param queryTables Tables referenced in the query (name -> plan node)
   * @param viewTables  Tables referenced in the view (name -> plan node)
   * @return A sequence of possible TableMappings (multiple for self-joins)
   */
  def generate(
      queryPlan: LogicalPlan,
      viewPlan: LogicalPlan,
      queryTables: Map[String, LogicalPlan],
      viewTables: Map[String, LogicalPlan]): Seq[TableMapping] = {
    // Find common table names
    val commonTableNames = queryTables.keySet.intersect(viewTables.keySet)

    if (commonTableNames.isEmpty) {
      return Seq.empty
    }

    // For each common table, build attribute mappings
    val tableAttrMappings = commonTableNames.map { tableName =>
      val queryAttrs = queryTables(tableName).output
      val viewAttrs = viewTables(tableName).output
      tableName -> buildAttributeMapping(queryAttrs, viewAttrs)
    }.toMap

    // Build the complete mapping
    val queryToViewTable = commonTableNames.map(name => name -> name).toMap

    val queryToViewAttr = tableAttrMappings.flatMap { case (_, mapping) =>
      mapping.queryToView
    }

    val viewToQueryAttr = tableAttrMappings.flatMap { case (_, mapping) =>
      mapping.viewToQuery
    }

    Seq(TableMapping(queryToViewTable, queryToViewAttr, viewToQueryAttr))
  }

  /**
   * Build an attribute-level mapping between two sets of attributes
   * by matching on attribute name.
   */
  private def buildAttributeMapping(
      queryAttrs: Seq[Attribute],
      viewAttrs: Seq[Attribute]): AttributeMapping = {
    val queryToView = scala.collection.mutable.Map[Attribute, Attribute]()
    val viewToQuery = scala.collection.mutable.Map[Attribute, Attribute]()

    for {
      qAttr <- queryAttrs
      vAttr <- viewAttrs
      if qAttr.name == vAttr.name
    } {
      queryToView(qAttr) = vAttr
      viewToQuery(vAttr) = qAttr
    }

    AttributeMapping(queryToView.toMap, viewToQuery.toMap)
  }

  /**
   * Translate an expression from the query namespace to the view namespace
   * using a table mapping.
   */
  def translateExpression(
      expr: Expression,
      mapping: TableMapping): Expression = {
    expr.transform {
      case attr: Attribute if mapping.queryToViewAttr.contains(attr) =>
        mapping.queryToViewAttr(attr)
    }
  }

  /**
   * Translate an expression from the view namespace to the query namespace
   * using a table mapping.
   */
  def translateExpressionReverse(
      expr: Expression,
      mapping: TableMapping): Expression = {
    expr.transform {
      case attr: Attribute if mapping.viewToQueryAttr.contains(attr) =>
        mapping.viewToQueryAttr(attr)
    }
  }
}

/**
 * Attribute-level mapping between query and view for a single table.
 */
private case class AttributeMapping(
    queryToView: Map[Attribute, Attribute],
    viewToQuery: Map[Attribute, Attribute]
)