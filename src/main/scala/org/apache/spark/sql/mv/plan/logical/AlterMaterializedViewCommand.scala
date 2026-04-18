package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.mv.model.MVIdentifier

/**
 * Logical plan node for ALTER MATERIALIZED VIEW command.
 * Supports RENAME TO, SET TBLPROPERTIES, and UNSET TBLPROPERTIES.
 */
case class AlterMaterializedViewCommand(
    identifier: MVIdentifier,
    renameTo: Option[MVIdentifier],
    setProperties: Map[String, String],
    unsetProperties: Seq[String],
    ifExists: Boolean
) extends Command {

  override def children: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = Nil

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    val actions = Seq(
      renameTo.map(r => s"RENAME TO ${r.quotedString}"),
      if (setProperties.nonEmpty) Some(s"SET TBLPROPERTIES $setProperties") else None,
      if (unsetProperties.nonEmpty) Some(s"UNSET TBLPROPERTIES $unsetProperties") else None
    ).flatten.mkString(", ")
    s"AlterMaterializedView ${identifier.quotedString} $actions"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}