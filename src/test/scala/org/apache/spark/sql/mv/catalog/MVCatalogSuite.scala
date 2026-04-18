package org.apache.spark.sql.mv.catalog

import org.apache.spark.sql.mv.model._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MVCatalogSuite extends AnyFunSuite with Matchers {

  private def newCatalog: MVCatalog = new MVCatalog(new MVInMemoryCatalogStore)

  private def sampleDefinition(
      name: String,
      db: Option[String] = None): MVDefinition = {
    MVDefinition(
      identifier = MVIdentifier(db, name),
      queryPlan = org.apache.spark.sql.catalyst.plans.logical.OneRowRelation(),
      outputSchema = Seq.empty,
      tableName = s"__mv_${db.getOrElse("default")}_$name",
      createdAt = System.currentTimeMillis(),
      lastRefreshedAt = System.currentTimeMillis()
    )
  }

  test("create and get MV definition") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    catalog.get(defn.identifier) shouldBe Some(defn)
  }

  test("create MV that already exists should throw") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    val thrown = the[IllegalArgumentException] thrownBy {
      catalog.create(sampleDefinition("mv1"))
    }
    thrown.getMessage should include("already exists")
  }

  test("create MV with ifNotExists should not throw when exists") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    noException should be thrownBy {
      catalog.create(sampleDefinition("mv1"), ifNotExists = true)
    }
  }

  test("get non-existent MV should return None") {
    val catalog = newCatalog
    catalog.get(MVIdentifier(None, "nonexistent")) shouldBe None
  }

  test("getOrThrow should throw for non-existent MV") {
    val catalog = newCatalog
    val thrown = the[IllegalArgumentException] thrownBy {
      catalog.getOrThrow(MVIdentifier(None, "nonexistent"))
    }
    thrown.getMessage should include("not found")
  }

  test("remove existing MV") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    catalog.remove(defn.identifier) shouldBe true
    catalog.get(defn.identifier) shouldBe None
  }

  test("remove non-existent MV without ifExists should throw") {
    val catalog = newCatalog
    val thrown = the[IllegalArgumentException] thrownBy {
      catalog.remove(MVIdentifier(None, "nonexistent"), ifExists = false)
    }
    thrown.getMessage should include("not found")
  }

  test("remove non-existent MV with ifExists should not throw") {
    val catalog = newCatalog
    noException should be thrownBy {
      catalog.remove(MVIdentifier(None, "nonexistent"), ifExists = true)
    }
  }

  test("update MV definition") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    val updated = catalog.update(defn.identifier) { d =>
      d.copy(comment = Some("updated comment"))
    }
    updated.comment shouldBe Some("updated comment")
    catalog.get(defn.identifier).get.comment shouldBe Some("updated comment")
  }

  test("rename MV") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)
    val newId = MVIdentifier(None, "mv2")
    catalog.rename(defn.identifier, newId)
    catalog.get(defn.identifier) shouldBe None
    catalog.get(newId) shouldBe defined
    catalog.get(newId).get.identifier.name shouldBe "mv2"
  }

  test("list all MVs") {
    val catalog = newCatalog
    catalog.create(sampleDefinition("mv1"))
    catalog.create(sampleDefinition("mv2"))
    catalog.listAll().map(_.identifier.name) should contain theSameElementsAs Seq("mv1", "mv2")
  }

  test("list MVs by namespace") {
    val catalog = newCatalog
    catalog.create(sampleDefinition("mv1", Some("db1")))
    catalog.create(sampleDefinition("mv2", Some("db2")))
    catalog.create(sampleDefinition("mv3", Some("db1")))
    catalog.listByNamespace("db1").map(_.identifier.name) should contain theSameElementsAs Seq("mv1", "mv3")
  }

  test("list rewritable MVs (FRESH only)") {
    val catalog = newCatalog
    val fresh = sampleDefinition("mv1").copy(state = MVState.FRESH)
    val stale = sampleDefinition("mv2").copy(state = MVState.STALE)
    val disabled = sampleDefinition("mv3").copy(state = MVState.DISABLED)
    catalog.create(fresh)
    catalog.create(stale)
    catalog.create(disabled)
    catalog.listRewritable.map(_.identifier.name) shouldBe Seq("mv1")
  }

  test("exists check") {
    val catalog = newCatalog
    catalog.create(sampleDefinition("mv1"))
    catalog.exists(MVIdentifier(None, "mv1")) shouldBe true
    catalog.exists(MVIdentifier(None, "nonexistent")) shouldBe false
  }

  test("MV state transitions") {
    val catalog = newCatalog
    val defn = sampleDefinition("mv1")
    catalog.create(defn)

    // FRESH -> STALE
    catalog.update(defn.identifier)(_.withState(MVState.STALE))
    catalog.get(defn.identifier).get.state shouldBe MVState.STALE
    catalog.listRewritable shouldBe empty

    // STALE -> FRESH
    catalog.update(defn.identifier)(_.withRefreshed())
    catalog.get(defn.identifier).get.state shouldBe MVState.FRESH
    catalog.listRewritable should have size 1
  }
}