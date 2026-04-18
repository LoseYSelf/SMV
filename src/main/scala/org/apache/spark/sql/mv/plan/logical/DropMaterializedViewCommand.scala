package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.mv.model.MVIdentifier

/**
 * Logical plan node for DROP MATERIALIZED VIEW command.
 */
case class DropMaterializedViewCommand(
    identifier: MVIdentifier,
    ifExists: Boolean
) extends Command {

  override def children: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = Nil

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"DropMaterializedView ${identifier.quotedString}"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}