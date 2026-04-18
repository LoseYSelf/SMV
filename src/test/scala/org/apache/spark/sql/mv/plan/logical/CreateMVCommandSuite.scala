package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.mv.model.MVIdentifier
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CreateMVCommandSuite extends AnyFunSuite with Matchers {

  test("CreateMaterializedViewCommand simpleString") {
    val cmd = CreateMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1"),
      queryText = "SELECT * FROM t",
      ifNotExists = false,
      orReplace = false,
      comment = None,
      provider = Some("parquet"),
      options = Map.empty,
      partitionColumns = Seq.empty,
      tableProperties = Map.empty
    )
    cmd.simpleString(10) should include("mv1")
  }

  test("CreateMaterializedViewCommand output is empty") {
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
    cmd.output shouldBe empty
  }
}