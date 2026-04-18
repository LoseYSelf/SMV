package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.mv.model.MVIdentifier

/**
 * Logical plan node for CREATE MATERIALIZED VIEW command.
 *
 * The queryText is parsed and analyzed at execution time using Spark's
 * native parser, and the resulting plan is stored in the MV catalog.
 */
case class CreateMaterializedViewCommand(
    identifier: MVIdentifier,
    queryText: String,
    ifNotExists: Boolean,
    orReplace: Boolean,
    comment: Option[String],
    provider: Option[String],
    options: Map[String, String],
    partitionColumns: Seq[String],
    tableProperties: Map[String, String]
) extends Command {

  override def children: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = Nil

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"CreateMaterializedView ${identifier.quotedString}"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}