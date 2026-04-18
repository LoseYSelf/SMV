package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.mv.model.MVIdentifier
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RefreshMVCommandSuite extends AnyFunSuite with Matchers {

  test("RefreshMaterializedViewCommand simpleString") {
    val cmd = RefreshMaterializedViewCommand(
      identifier = MVIdentifier(Some("db"), "mv1")
    )
    cmd.simpleString(10) should include("mv1")
  }

  test("RefreshMaterializedViewCommand output is empty") {
    val cmd = RefreshMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1")
    )
    cmd.output shouldBe empty
  }
}