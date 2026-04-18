package org.apache.spark.sql.mv.parser

import org.apache.spark.sql.mv.model.MVIdentifier
import org.apache.spark.sql.mv.plan.logical._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MVSqlParserSuite extends AnyFunSuite with Matchers {

  test("MVIdentifier unquotedString with database") {
    val id = MVIdentifier(Some("db"), "mv1")
    id.unquotedString shouldBe "db.mv1"
  }

  test("MVIdentifier unquotedString without database") {
    val id = MVIdentifier(None, "mv1")
    id.unquotedString shouldBe "mv1"
  }

  test("MVIdentifier quotedString") {
    val id = MVIdentifier(Some("db"), "mv1")
    id.quotedString shouldBe "`db`.`mv1`"
  }

  test("MVIdentifier quotedString without database") {
    val id = MVIdentifier(None, "mv1")
    id.quotedString shouldBe "`mv1`"
  }

  test("CreateMaterializedViewCommand basic fields") {
    val cmd = CreateMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      queryText = "SELECT * FROM t1",
      ifNotExists = false,
      orReplace = false,
      comment = Some("test mv"),
      provider = Some("parquet"),
      options = Map("path" -> "/tmp/mv1"),
      partitionColumns = Seq("date"),
      tableProperties = Map("key" -> "val")
    )
    cmd.identifier.name shouldBe "mv1"
    cmd.queryText shouldBe "SELECT * FROM t1"
    cmd.ifNotExists shouldBe false
    cmd.orReplace shouldBe false
    cmd.comment shouldBe Some("test mv")
    cmd.provider shouldBe Some("parquet")
    cmd.options shouldBe Map("path" -> "/tmp/mv1")
    cmd.partitionColumns shouldBe Seq("date")
    cmd.tableProperties shouldBe Map("key" -> "val")
  }

  test("CreateMaterializedViewCommand with OR REPLACE and IF NOT EXISTS") {
    val cmd = CreateMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1"),
      queryText = "SELECT 1",
      ifNotExists = true,
      orReplace = true,
      comment = None,
      provider = None,
      options = Map.empty,
      partitionColumns = Seq.empty,
      tableProperties = Map.empty
    )
    cmd.ifNotExists shouldBe true
    cmd.orReplace shouldBe true
  }

  test("DropMaterializedViewCommand basic fields") {
    val cmd = DropMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      ifExists = true
    )
    cmd.ifExists shouldBe true
  }

  test("AlterMaterializedViewCommand with SET TBLPROPERTIES") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = None,
      setProperties = Map("key1" -> "val1", "key2" -> "val2"),
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.setProperties should have size 2
    cmd.renameTo shouldBe None
  }

  test("AlterMaterializedViewCommand with UNSET TBLPROPERTIES") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = None,
      setProperties = Map.empty,
      unsetProperties = Seq("key1", "key2"),
      ifExists = true
    )
    cmd.unsetProperties should have size 2
    cmd.ifExists shouldBe true
  }

  test("RefreshMaterializedViewCommand basic fields") {
    val cmd = RefreshMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1")
    )
    cmd.identifier.database shouldBe Some("db")
  }

  test("ShowMaterializedViewsCommand with namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = Some("mydb"))
    cmd.namespace shouldBe Some("mydb")
  }

  test("ShowMaterializedViewsCommand without namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = None)
    cmd.namespace shouldBe None
  }
}