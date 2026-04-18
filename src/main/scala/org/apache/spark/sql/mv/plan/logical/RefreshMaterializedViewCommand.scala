package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.mv.model.MVIdentifier

/**
 * Logical plan node for REFRESH MATERIALIZED VIEW command.
 * Recomputes the MV data by re-executing the defining query.
 */
case class RefreshMaterializedViewCommand(
    identifier: MVIdentifier
) extends Command {

  override def children: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = Nil

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"RefreshMaterializedView ${identifier.quotedString}"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}