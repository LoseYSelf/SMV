package org.apache.spark.sql.mv.rewrite.util

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation

object ExpressionLineage {

  def trace(expr: Expression, plan: LogicalPlan): Set[Attribute] = {
    expr.references.toSet.flatMap { attr: Attribute =>
      findSourceAttribute(attr, plan)
    }
  }

  def findSourceAttribute(attr: Attribute, plan: LogicalPlan): Set[Attribute] = {
    if (plan.output.exists(_.semanticEquals(attr))) {
      plan match {
        case p: LogicalRelation =>
          Set(attr)
        case _: OneRowRelation =>
          Set(attr)
        case _: LocalRelation =>
          Set(attr)
        case Project(projectList, child) =>
          projectList.find(_.toAttribute.semanticEquals(attr)) match {
            case Some(namedExpr) => trace(namedExpr, child)
            case None => findSourceAttribute(attr, child)
          }
        case Filter(_, child) =>
          findSourceAttribute(attr, child)
        case Aggregate(_, aggregateExprs, child) =>
          aggregateExprs.find(_.toAttribute.semanticEquals(attr)) match {
            case Some(namedExpr: NamedExpression) => trace(namedExpr, child)
            case None => Set(attr)
          }
        case Join(left, right, _, _, _) =>
          val fromLeft = findSourceAttribute(attr, left)
          if (fromLeft.nonEmpty) fromLeft
          else findSourceAttribute(attr, right)
        case SubqueryAlias(_, child) =>
          findSourceAttribute(attr, child)
        case _ =>
          plan.children.flatMap(findSourceAttribute(attr, _)).toSet
      }
    } else {
      Set.empty
    }
  }

  def extractTableReferences(plan: LogicalPlan): Map[String, LogicalPlan] = {
    plan.collect {
      case rel: LogicalRelation =>
        val tableName = rel.catalogTable.map(_.identifier.unquotedString)
          .getOrElse(s"table_${rel.output.headOption.map(_.qualifier.mkString(".")).getOrElse("unknown")}")
        tableName -> rel
    }.toMap
  }

  def isSPJGPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Filter | _: Project | _: Join | _: Aggregate |
           _: LogicalRelation | _: OneRowRelation | _: LocalRelation =>
        plan.children.forall(isSPJGPlan)
      case SubqueryAlias(_, child) =>
        isSPJGPlan(child)
      case _ =>
        false
    }
  }

  def extractSPJGComponents(plan: LogicalPlan): Option[SPJGComponents] = {
    if (!isSPJGPlan(plan)) return None

    var filters: Seq[Expression] = Seq.empty
    var projections: Seq[NamedExpression] = Seq.empty
    var joins: Seq[Join] = Seq.empty
    var aggregate: Option[Aggregate] = None
    var tables: Map[String, LogicalPlan] = Map.empty

    def traverse(p: LogicalPlan): Unit = p match {
      case Filter(condition, child) =>
        filters = filters :+ condition
        traverse(child)
      case Project(projectList, child) =>
        if (projections.isEmpty) projections = projectList
        traverse(child)
      case join: Join =>
        joins = joins :+ join
        traverse(join.left)
        traverse(join.right)
      case agg: Aggregate =>
        aggregate = Some(agg)
        traverse(agg.child)
      case rel: LogicalRelation =>
        val tableName = rel.catalogTable.map(_.identifier.unquotedString)
          .getOrElse(s"table_${tables.size}")
        tables = tables + (tableName -> rel)
      case SubqueryAlias(_, child) =>
        traverse(child)
      case _ =>
        p.children.foreach(traverse)
    }

    traverse(plan)

    Some(SPJGComponents(
      filters = filters,
      projections = projections,
      joins = joins,
      aggregate = aggregate,
      tables = tables
    ))
  }
}

case class SPJGComponents(
    filters: Seq[Expression],
    projections: Seq[NamedExpression],
    joins: Seq[Join],
    aggregate: Option[Aggregate],
    tables: Map[String, LogicalPlan]
)