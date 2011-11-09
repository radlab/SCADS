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
      output("ranksep=0.2;")
      f
    }
  }

  case class DotNode(id: String)

  protected def outputNode(label: String, shape: String = "plaintext", fillcolor: String = "azure3", style: String = "none", fontSize: Int = 12, children: Seq[DotNode] = Nil): DotNode = {
    val node = DotNode("node" + nextId)
    output(node.id, "[label=", quote(label), ", shape=", shape, ", fillcolor=", fillcolor, ", style=", style,  ", fontsize=" + fontSize + "];")
    children.foreach(outputEdge(_, node))
    return node
  }

  protected def outputEdge(src: DotNode, dest: DotNode, label: String = "", color: String = "black", arrowsize: Double = 0.5): DotNode = {
    output(src.id, " -> ", dest.id, "[label=", quote(label), ", color=", quote(color), ", arrowsize=", arrowsize.toString, "];")
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
      outputNode(selectionSym + "\n" + prettyPrint(predicate),
        children = Seq(generatePlan(child)))
    case Sort(attributes, ascending, child) =>
      outputNode("sort " + (if (ascending) "asc" else "desc") + "\n" + attributes.map(prettyPrint).mkString(","),
        children = Seq(generatePlan(child)))
    case StopAfter(count, child) =>
      outputNode("stop\n" + prettyPrint(count),
        children = Seq(generatePlan(child)))
    case DataStopAfter(count, child) =>
      outputNode("data stop\n" + prettyPrint(count),
        children = Seq(generatePlan(child)))
    case Join(left, right) =>
      outputNode(joinSym,
        fontSize =36,
        children = Seq(generatePlan(left), generatePlan(right)))
    case Relation(ns) =>
      outputNode(ns.name,
        shape="box")
  }

  protected def prettyPrint(p: Predicate): String = p match {
    case EqualityPredicate(v1, v2) => prettyPrint(v1) + " = " + prettyPrint(v2)
  }

  protected def prettyPrint(v: Value): String = v match {
    case UnboundAttributeValue(name) => name
    case ConstantValue(c) => c.toString
  }

  protected def prettyPrint(v: Limit): String = v match {
    case FixedLimit(n) => n.toString
  }
}