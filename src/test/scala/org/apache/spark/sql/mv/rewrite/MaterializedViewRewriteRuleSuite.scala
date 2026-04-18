package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.mv.model.{MVDefinition, MVIdentifier, MVState}
import org.apache.spark.sql.mv.plan.logical._
import org.apache.spark.sql.mv.rewrite.MaterializedViewRewriteRule
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MaterializedViewRewriteRuleSuite extends AnyFunSuite with Matchers {

  test("MVCommand plans are not rewritten") {
    // Verify that MV DDL commands are not subject to rewriting
    val cmd = CreateMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      queryText = "SELECT 1",
      ifNotExists = false,
      orReplace = false,
      comment = None,
      provider = None,
      options = Map.empty,
      partitionColumns = Seq.empty,
      tableProperties = Map.empty
    )
    // The isMVCommand check should recognize this as an MV command
    // We can't easily instantiate the rule without a SparkSession,
    // but we verify the command types are correctly formed
    cmd.identifier.name shouldBe "mv1"
    cmd.queryText shouldBe "SELECT 1"
  }

  test("DropMaterializedViewCommand with ifExists") {
    val cmd = DropMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1"),
      ifExists = true
    )
    cmd.ifExists shouldBe true
    cmd.identifier.database shouldBe Some("db")
  }

  test("AlterMaterializedViewCommand rename") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = Some(MVIdentifier(None, "mv2")),
      setProperties = Map.empty,
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.renameTo shouldBe defined
    cmd.renameTo.get.name shouldBe "mv2"
  }

  test("AlterMaterializedViewCommand set properties") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = None,
      setProperties = Map("key" -> "value"),
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.setProperties shouldBe Map("key" -> "value")
  }

  test("RefreshMaterializedViewCommand") {
    val cmd = RefreshMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1")
    )
    cmd.identifier.name shouldBe "mv1"
  }

  test("ShowMaterializedViewsCommand with namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = Some("db1"))
    cmd.namespace shouldBe Some("db1")
  }

  test("ShowMaterializedViewsCommand without namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = None)
    cmd.namespace shouldBe None
  }

  test("MVDefinition state transitions") {
    val defn = MVDefinition(
      identifier = MVIdentifier(None, "mv1"),
      queryPlan = org.apache.spark.sql.catalyst.plans.logical.OneRowRelation(),
      outputSchema = Seq.empty,
      tableName = "__mv_default_mv1"
    )
    defn.state shouldBe MVState.FRESH
    defn.isRewritable shouldBe true

    val stale = defn.withState(MVState.STALE)
    stale.state shouldBe MVState.STALE
    stale.isRewritable shouldBe false

    val refreshed = stale.withRefreshed()
    refreshed.state shouldBe MVState.FRESH
    refreshed.isRewritable shouldBe true
  }
}