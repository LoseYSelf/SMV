package org.apache.spark.sql.mv.rewrite

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualTo}
import org.apache.spark.sql.mv.rewrite.spjg.EquivalenceClasses
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class EquivalenceClassesSuite extends AnyFunSuite with Matchers {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType)()

  test("empty equivalence classes") {
    val ec = EquivalenceClasses()
    ec.size shouldBe 0
    ec.getEquivalenceClasses shouldBe empty
  }

  test("single equality creates one equivalence class") {
    val ec = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    ec.addEquality(EqualTo(a, b))
    ec.size shouldBe 1
    ec.areEquivalent(a, b) shouldBe true
  }

  test("two separate equalities create two equivalence classes") {
    val ec = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val d = attr("d")
    ec.addEquality(EqualTo(a, b))
    ec.addEquality(EqualTo(c, d))
    ec.size shouldBe 2
    ec.areEquivalent(a, b) shouldBe true
    ec.areEquivalent(c, d) shouldBe true
    ec.areEquivalent(a, c) shouldBe false
  }

  test("transitive equality merges classes") {
    val ec = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    ec.addEquality(EqualTo(a, b))
    ec.addEquality(EqualTo(b, c))
    ec.size shouldBe 1
    ec.areEquivalent(a, c) shouldBe true
  }

  test("isCoveredBy - same classes") {
    val ec1 = EquivalenceClasses()
    val ec2 = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    ec1.addEquality(EqualTo(a, b))
    ec2.addEquality(EqualTo(a, b))
    ec1.isCoveredBy(ec2) shouldBe true
  }

  test("isCoveredBy - subset classes") {
    val ec1 = EquivalenceClasses()
    val ec2 = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    ec1.addEquality(EqualTo(a, b))
    ec2.addEquality(EqualTo(a, b))
    ec2.addEquality(EqualTo(a, c))
    ec1.isCoveredBy(ec2) shouldBe true
  }

  test("isCoveredBy - not covered when extra equivalence class") {
    val ec1 = EquivalenceClasses()
    val ec2 = EquivalenceClasses()
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val d = attr("d")
    ec1.addEquality(EqualTo(a, b))
    ec1.addEquality(EqualTo(c, d))
    ec2.addEquality(EqualTo(a, b))
    // ec1 has an extra equivalence class (c=d) that ec2 doesn't cover
    ec1.isCoveredBy(ec2) shouldBe false
  }

  test("construct from sequence of equalities") {
    val a = attr("a")
    val b = attr("b")
    val c = attr("c")
    val ec = EquivalenceClasses(Seq(
      EqualTo(a, b),
      EqualTo(b, c)
    ))
    ec.size shouldBe 1
    ec.areEquivalent(a, c) shouldBe true
  }
}