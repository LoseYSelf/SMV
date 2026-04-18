package org.apache.spark.sql.mv.rewrite.spjg

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Coalesce, Divide, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.IntegerType

object AggregateRollup {

  case class RollupResult(
      canRollup: Boolean,
      rollupExprs: Seq[NamedExpression],
      outputExprs: Seq[NamedExpression]
  )

  /**
   * Represents the mapping for a single aggregate function rollup.
   * For simple aggregates (SUM, MIN, MAX, COUNT), rollupExprs has one element.
   * For AVG, rollupExprs has two elements (SUM + COUNT).
   */
  case class AggregateMapping(
      rollupExprs: Seq[NamedExpression],
      outputExpr: NamedExpression
  )

  def isGroupingSubset(queryGrouping: Seq[Expression], mvGrouping: Seq[Expression]): Boolean = {
    queryGrouping.forall { qExpr =>
      mvGrouping.exists(_.semanticEquals(qExpr))
    }
  }

  def rollupAggregate(
      queryAggExpr: AggregateExpression,
      mvOutput: Seq[Attribute]): Option[AggregateMapping] = {
    queryAggExpr.aggregateFunction match {
      case Sum(child, _) =>
        findMVAggregateColumn(child, "sum", mvOutput).map { mvAttr =>
          val rollupExpr = AggregateExpression(Sum(mvAttr), queryAggExpr.mode, queryAggExpr.isDistinct)
          val rollupAlias = Alias(rollupExpr, "rollup_sum")()
          val outputAttr = AttributeReference("rollup_sum", rollupExpr.dataType, nullable = true)()
          val outputExpr = Alias(outputAttr, "sum")()
          AggregateMapping(Seq(rollupAlias), outputExpr)
        }

      case Min(child) =>
        findMVAggregateColumn(child, "min", mvOutput).map { mvAttr =>
          val rollupExpr = AggregateExpression(Min(mvAttr), queryAggExpr.mode, queryAggExpr.isDistinct)
          val rollupAlias = Alias(rollupExpr, "rollup_min")()
          val outputAttr = AttributeReference("rollup_min", rollupExpr.dataType, nullable = true)()
          val outputExpr = Alias(outputAttr, "min")()
          AggregateMapping(Seq(rollupAlias), outputExpr)
        }

      case Max(child) =>
        findMVAggregateColumn(child, "max", mvOutput).map { mvAttr =>
          val rollupExpr = AggregateExpression(Max(mvAttr), queryAggExpr.mode, queryAggExpr.isDistinct)
          val rollupAlias = Alias(rollupExpr, "rollup_max")()
          val outputAttr = AttributeReference("rollup_max", rollupExpr.dataType, nullable = true)()
          val outputExpr = Alias(outputAttr, "max")()
          AggregateMapping(Seq(rollupAlias), outputExpr)
        }

      case Count(_) =>
        findMVAggregateColumnForCount(mvOutput).map { mvAttr =>
          val sumExpr = AggregateExpression(Sum(mvAttr), queryAggExpr.mode, queryAggExpr.isDistinct)
          val rollupAlias = Alias(sumExpr, "rollup_count")()
          val outputAttr = AttributeReference("rollup_count", sumExpr.dataType, nullable = true)()
          val outputExpr = Alias(
            Coalesce(Seq(outputAttr, Literal(0L))),
            "count")()
          AggregateMapping(Seq(rollupAlias), outputExpr)
        }

      case Average(child, _) =>
        val mvSumOpt = findMVAggregateColumn(child, "sum", mvOutput)
        val mvCountOpt = findMVAggregateColumnForCount(mvOutput)
        (mvSumOpt, mvCountOpt) match {
          case (Some(mvSum), Some(mvCount)) =>
            val sumRollup = AggregateExpression(Sum(mvSum), queryAggExpr.mode, queryAggExpr.isDistinct)
            val countRollup = AggregateExpression(Sum(mvCount), queryAggExpr.mode, queryAggExpr.isDistinct)
            val sumAlias = Alias(sumRollup, "rollup_avg_sum")()
            val countAlias = Alias(countRollup, "rollup_avg_count")()
            val outputExpr = Alias(
              Divide(sumAlias.toAttribute, countAlias.toAttribute),
              "avg")()
            Some(AggregateMapping(Seq(sumAlias, countAlias), outputExpr))
          case _ =>
            None
        }

      case _ =>
        None
    }
  }

  private def findMVAggregateColumn(
      child: Expression,
      aggName: String,
      mvOutput: Seq[Attribute]): Option[Attribute] = {
    mvOutput.find { attr =>
      attr.name.toLowerCase.contains(aggName)
    }
  }

  private def findMVAggregateColumnForCount(
      mvOutput: Seq[Attribute]): Option[Attribute] = {
    mvOutput.find { attr =>
      attr.name.toLowerCase.contains("count")
    }
  }

  def computeRollup(
      queryAggregate: Aggregate,
      mvAggregate: Aggregate,
      mvOutput: Seq[Attribute]): RollupResult = {
    val queryGrouping = queryAggregate.groupingExpressions
    val mvGrouping = mvAggregate.groupingExpressions

    if (!isGroupingSubset(queryGrouping, mvGrouping)) {
      return RollupResult(canRollup = false, Seq.empty, Seq.empty)
    }

    if (queryGrouping.length == mvGrouping.length) {
      return RollupResult(
        canRollup = true,
        rollupExprs = queryAggregate.aggregateExpressions,
        outputExprs = queryAggregate.aggregateExpressions
      )
    }

    val allRollupExprs = scala.collection.mutable.ArrayBuffer[NamedExpression]()
    val allOutputExprs = scala.collection.mutable.ArrayBuffer[NamedExpression]()
    var allMatched = true

    queryAggregate.aggregateExpressions.foreach { expr =>
      if (!allMatched) return null // early exit on failure

      val mappingOpt = expr match {
        case alias: Alias =>
          alias.child match {
            case aggExpr: AggregateExpression =>
              rollupAggregate(aggExpr, mvOutput).map { mapping =>
                (mapping, Some(alias.name))
              }
            case _ => None
          }
        case aggExpr: AggregateExpression =>
          rollupAggregate(aggExpr, mvOutput).map { mapping =>
            (mapping, None)
          }
        case _ => None
      }

      mappingOpt match {
        case Some((mapping, aliasNameOpt)) =>
          allRollupExprs ++= mapping.rollupExprs
          aliasNameOpt match {
            case Some(name) =>
              allOutputExprs += Alias(mapping.outputExpr, name)()
            case None =>
              allOutputExprs += mapping.outputExpr
          }
        case None =>
          allMatched = false
      }
    }

    if (!allMatched) {
      RollupResult(canRollup = false, Seq.empty, Seq.empty)
    } else {
      RollupResult(
        canRollup = true,
        rollupExprs = allRollupExprs.toSeq,
        outputExprs = allOutputExprs.toSeq
      )
    }
  }
}