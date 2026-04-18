package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.mv.model.MVIdentifier
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DropMVCommandSuite extends AnyFunSuite with Matchers {

  test("DropMaterializedViewCommand simpleString") {
    val cmd = DropMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1"),
      ifExists = false
    )
    cmd.simpleString(10) should include("mv1")
  }

  test("DropMaterializedViewCommand with ifExists") {
    val cmd = DropMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      ifExists = true
    )
    cmd.ifExists shouldBe true
  }

  test("DropMaterializedViewCommand output is empty") {
    val cmd = DropMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      ifExists = false
    )
    cmd.output shouldBe empty
  }
}