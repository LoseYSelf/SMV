package org.apache.spark.sql.mv.plan.logical

import org.apache.spark.sql.mv.model.MVIdentifier
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AlterMVCommandSuite extends AnyFunSuite with Matchers {

  test("AlterMaterializedViewCommand simpleString with rename") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = Some(MVIdentifier(None, "mv2")),
      setProperties = Map.empty,
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.simpleString(10) should include("RENAME TO")
  }

  test("AlterMaterializedViewCommand simpleString with set properties") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = None,
      setProperties = Map("key" -> "val"),
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.simpleString(10) should include("SET TBLPROPERTIES")
  }

  test("AlterMaterializedViewCommand output is empty") {
    val cmd = AlterMaterializedViewCommand(
      identifier = MVIdentifier(None, "mv1"),
      renameTo = None,
      setProperties = Map.empty,
      unsetProperties = Seq.empty,
      ifExists = false
    )
    cmd.output shouldBe empty
  }
}