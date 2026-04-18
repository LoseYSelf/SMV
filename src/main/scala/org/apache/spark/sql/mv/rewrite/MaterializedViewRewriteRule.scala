package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.mv.catalog.MVCatalog
import org.apache.spark.sql.mv.rewrite.spjg.SPJGRewriter
import org.apache.spark.sql.mv.rewrite.util.ExpressionLineage

/**
 * Optimizer rule that rewrites queries to use materialized views.
 *
 * This rule is injected via MVSessionExtensions and runs during the
 * "Operator Optimization" phase of Spark's Catalyst optimizer.
 *
 * Strategy:
 * 1. Skip if the plan is already an MV command (avoid recursive rewriting)
 * 2. For each eligible (FRESH) materialized view in the catalog:
 *    a. Attempt SPJG rewriting
 *    b. If successful, return the rewritten plan immediately
 * 3. If no MV can be used, return the original plan unchanged
 */
class MaterializedViewRewriteRule(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  override val ruleName: String = "MaterializedViewRewrite"

  // The MV catalog instance, obtained from the session's catalog manager
  private lazy val mvCatalog: MVCatalog = {
    MVCatalogManager.getCatalog(sparkSession)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Skip MV command plans to avoid recursive rewriting
    if (isMVCommand(plan)) return plan

    // Skip non-SPJG plans early
    if (!ExpressionLineage.isSPJGPlan(plan)) return plan

    // Get all rewritable MVs
    val candidates = mvCatalog.listRewritable
    if (candidates.isEmpty) return plan

    // Try each MV until one succeeds
    candidates.foreach { mvDef =>
      val result = SPJGRewriter.rewrite(plan, mvDef)
      if (result.success && result.rewrittenPlan.isDefined) {
        return result.rewrittenPlan.get
      }
    }

    // No suitable MV found, return original plan
    plan
  }

  /**
   * Check if a plan is an MV-related command that should not be rewritten.
   */
  private def isMVCommand(plan: LogicalPlan): Boolean = {
    plan match {
      case _: org.apache.spark.sql.mv.plan.logical.CreateMaterializedViewCommand => true
      case _: org.apache.spark.sql.mv.plan.logical.AlterMaterializedViewCommand => true
      case _: org.apache.spark.sql.mv.plan.logical.DropMaterializedViewCommand => true
      case _: org.apache.spark.sql.mv.plan.logical.RefreshMaterializedViewCommand => true
      case _: org.apache.spark.sql.mv.plan.logical.ShowMaterializedViewsCommand => true
      case _ => false
    }
  }
}

/**
 * Manages the per-session MV catalog instances.
 * Ensures each SparkSession gets its own catalog instance.
 */
object MVCatalogManager {

  private val catalogs = new java.util.concurrent.ConcurrentHashMap[String, MVCatalog]()

  def getCatalog(sparkSession: SparkSession): MVCatalog = {
    val sessionId = sparkSession.sessionUUID
    catalogs.computeIfAbsent(sessionId, _ => new MVCatalog())
  }

  def removeCatalog(sparkSession: SparkSession): Unit = {
    catalogs.remove(sparkSession.sessionUUID)
  }
}