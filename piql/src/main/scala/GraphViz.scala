package edu.berkeley.cs.scads.piql
package viz

import java.io.File
import plans._

object Test {

  import edu.berkeley.cs.scads.piql.test.Relations._

  def main(args: Array[String]): Unit = {
    val query = (
      r2.where("f1".a === 0)
        .dataLimit(5)
        .join(r2Prime)
        .where("r2.f2".a === "r2Prime.f1".a)
        .sort("r2Prime.f2".a :: Nil)
        .limit(10)
      )

    println(GraphLogicalPlan(query))
    sys.exit()
  }
}

trait GraphViz extends FileGenerator {
  private val curId = new java.util.concurrent.atomic.AtomicInteger

  protected def nextId = curId.getAndIncrement()

  protected def digraph[A](name: String)(f: => A): A = {
    outputBraced("digraph %s".format(name)) {
      output("rankdir=BT;")
      f
    }
  }

  case class DotNode(id: String)

  protected def outputNode(label: String, shape: String = "box", fillcolor: String = "azure3", style: String = "none", children: List[DotNode] = Nil): DotNode = {
    val node = DotNode("node" + nextId)
    output(node.id, "[label=", quote(label), ", shape=", shape, ", fillcolor=", fillcolor, ", style=", style, "];")
    children.foreach(outputEdge(_, node))
    return node
  }

  protected def outputEdge(src: DotNode, dest: DotNode, label: String = "", color: String = "black"): DotNode = {
    output(src.id, " -> ", dest.id, "[label=", quote(label), ", color=", quote(color), "];")
    return dest
  }

  protected def outputCluster[A](label: String*)(func: => A): A = {
    outputBraced("subGraph ", "cluster" + nextId) {
      output("label=", quote(label.mkString), ";")
      func
    }
  }
}

object GraphLogicalPlan {
  def apply(plan: LogicalPlan): Unit = {
    val grapher = new GraphLogicalPlan(new File("test.dot"))
    grapher.generate(plan)
    grapher.finish
  }
}

protected class GraphLogicalPlan(val file: File) extends GraphViz {
  /* Need a function with return type Unit for Generator */
  protected def generate(plan: LogicalPlan): Unit = {
    digraph("QueryPlan") {
      generatePlan(plan)
    }
  }

  val joinSym = "&#9285;"
  val selectionSym = "&#963;"

  /* Recursive function to draw individual plan nodes */
  protected def generatePlan(plan: LogicalPlan): DotNode = plan match {
    case Selection(predicate, child) =>
      val childNode = generatePlan(child)
      val thisNode = outputNode(selectionSym + "\n" + prettyPrint(predicate))
      outputEdge(childNode, thisNode)
      thisNode
    case Sort(attributes, ascending, child) =>
      val childNode = generatePlan(child)
      val thisNode = outputNode("sort " + (if (ascending) "asc" else "desc") + "\n" + attributes.map(prettyPrint).mkString(","))
      outputEdge(childNode, thisNode)
      thisNode
    case StopAfter(count, child) =>
      val childNode = generatePlan (child)
      val thisNode = outputNode ("stop\n" + prettyPrint (count) )
      outputEdge (childNode, thisNode)
      thisNode
    case DataStopAfter(count, child) =>
      val childNode = generatePlan(child)
      val thisNode = outputNode("data stop\n" + prettyPrint(count))
      outputEdge(childNode, thisNode)
      thisNode
    case in: InnerNode =>
      val child = generatePlan(in.child)
      val thisNode = outputNode(in.toString)
      outputEdge(child, thisNode)
      thisNode
    case Join(left, right) =>
      val thisNode = outputNode(joinSym)
      val leftNode = generatePlan(left)
      val rightNode = generatePlan(right)
      outputEdge(leftNode, thisNode)
      outputEdge(rightNode, thisNode)
      thisNode
    case Relation(ns) =>
      outputNode(ns.name)
  }

  protected def prettyPrint(p: Predicate): String = p match {
    case EqualityPredicate(v1, v2) => prettyPrint(v1) + " = " + prettyPrint(v2)
  }

  protected def prettyPrint(v: Value): String = v match {
    case UnboundAttributeValue(name) => name
    case ConstantValue(c) => c.toString
  }
}