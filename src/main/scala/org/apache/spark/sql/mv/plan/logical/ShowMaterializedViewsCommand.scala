package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.mv.model.MVIdentifier
import org.apache.spark.sql.types.StringType

case class ShowMaterializedViewsCommand(
    namespace: Option[String]
) extends Command {

  override def children: Seq[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = Nil

  override def output: Seq[Attribute] = Seq(
    AttributeReference("database", StringType, nullable = false)(),
    AttributeReference("name", StringType, nullable = false)(),
    AttributeReference("state", StringType, nullable = false)(),
    AttributeReference("query", StringType, nullable = true)(),
    AttributeReference("provider", StringType, nullable = true)(),
    AttributeReference("comment", StringType, nullable = true)()
  )

  override def simpleString(maxFields: Int): String = {
    s"ShowMaterializedViews${namespace.map(n => s" IN $n").getOrElse("")}"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = this
}