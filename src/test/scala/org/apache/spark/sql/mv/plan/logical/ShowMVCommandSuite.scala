package org.apache.spark.sql.mv.plan.logical

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ShowMVCommandSuite extends AnyFunSuite with Matchers {

  test("ShowMaterializedViewsCommand simpleString with namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = Some("db1"))
    cmd.simpleString(10) should include("db1")
  }

  test("ShowMaterializedViewsCommand simpleString without namespace") {
    val cmd = ShowMaterializedViewsCommand(namespace = None)
    cmd.simpleString(10) should include("ShowMaterializedViews")
  }

  test("ShowMaterializedViewsCommand output has correct columns") {
    val cmd = ShowMaterializedViewsCommand(namespace = None)
    cmd.output should have size 6
    cmd.output.map(_.name) should contain theSameElementsInOrderAs Seq(
      "database", "name", "state", "query", "provider", "comment"
    )
  }
}