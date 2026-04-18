package org.apache.spark.sql.mv.plan

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.mv.model.MVDefinition

/**
 * A leaf logical plan node representing a scan of a materialized view.
 *
 * This node is produced by the MaterializedViewRewriteRule as the base
 * of the rewritten query plan. It references the underlying storage table
 * of the MV.
 *
 * During physical planning, this is resolved to a regular table scan
 * of the MV's underlying storage table.
 */
case class MVRelation(mvDefinition: MVDefinition) extends LeafNode {

  override def output: Seq[Attribute] = mvDefinition.outputSchema

  override def simpleString(maxFields: Int): String = {
    s"MVRelation(${mvDefinition.identifier.quotedString}, " +
      s"table=${mvDefinition.tableName}, " +
      s"state=${mvDefinition.state})"
  }

  /**
   * The underlying storage table name for this MV.
   */
  def storageTableName: String = mvDefinition.tableName
}