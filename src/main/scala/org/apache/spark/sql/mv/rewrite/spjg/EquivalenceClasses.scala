package org.apache.spark.sql.mv.rewrite.spjg

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.mv.rewrite.util.PredicateSplitter

class EquivalenceClasses (
    private val nodeToEC: java.util.HashMap[Expression, java.util.HashSet[Expression]]
) extends Serializable {

  def addEquality(eq: EqualTo): Unit = {
    val left = eq.left
    val right = eq.right
    union(left, right)
  }

  def addEqualities(eqs: Seq[EqualTo]): Unit = {
    eqs.foreach(addEquality)
  }

  private def find(expr: Expression): Expression = {
    if (!nodeToEC.containsKey(expr)) {
      expr
    } else {
      val parent = nodeToEC.get(expr).iterator().next()
      if (parent == expr) expr
      else find(parent)
    }
  }

  private def union(a: Expression, b: Expression): Unit = {
    val rootA = find(a)
    val rootB = find(b)
    if (rootA != rootB) {
      val setA = nodeToEC.getOrDefault(rootA, new java.util.HashSet[Expression]())
      val setB = nodeToEC.getOrDefault(rootB, new java.util.HashSet[Expression]())
      setA.add(rootB)
      setB.add(rootA)
      nodeToEC.put(rootA, setB)
      nodeToEC.put(rootB, setB)
    }
  }

  def areEquivalent(a: Expression, b: Expression): Boolean = {
    find(a) == find(b)
  }

  def getEquivalenceClasses: Seq[Set[Expression]] = {
    import scala.collection.JavaConverters._
    val classMap = new java.util.HashMap[Expression, java.util.HashSet[Expression]]()
    val iter = nodeToEC.keySet().iterator()
    while (iter.hasNext) {
      val expr = iter.next()
      val root = find(expr)
      if (!classMap.containsKey(root)) {
        classMap.put(root, new java.util.HashSet[Expression]())
      }
      classMap.get(root).add(expr)
    }
    classMap.values().asScala.toSeq.map(_.asScala.toSet)
  }

  def isCoveredBy(other: EquivalenceClasses, mappingFn: Expression => Expression): Boolean = {
    val thisClasses = this.getEquivalenceClasses
    val otherClasses = other.getEquivalenceClasses
    thisClasses.forall { thisClass =>
      val mappedClass = thisClass.map(mappingFn)
      otherClasses.exists { otherClass =>
        mappedClass.forall(otherClass.contains)
      }
    }
  }

  def isCoveredBy(other: EquivalenceClasses): Boolean = {
    isCoveredBy(other, identity)
  }

  def size: Int = getEquivalenceClasses.size
}

object EquivalenceClasses {
  def apply(): EquivalenceClasses = {
    new EquivalenceClasses(new java.util.HashMap[Expression, java.util.HashSet[Expression]]())
  }

  def apply(equalities: Seq[EqualTo]): EquivalenceClasses = {
    val ec = apply()
    ec.addEqualities(equalities)
    ec
  }
}