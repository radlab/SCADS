package edu.berkeley.cs.scads.piql
package viz

import plans._
import java.io.File
import sun.awt.SunHints.Value

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
  protected def outputNode(label: String, shape: String = "box", fillcolor: String = "azure3", style: String = "none", children:List[DotNode]= Nil): DotNode = {
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

  /* Recursive function to draw individual plan nodes */
  protected def generatePlan(plan: LogicalPlan): DotNode = plan match {
    case in: InnerNode =>
      val child = generatePlan(in.child)
      val thisNode = outputNode(in.toString)
      outputEdge(child, thisNode)
      thisNode
    case Join(left,right) =>
      outputNode(left.toString)
      outputNode(right.toString)
      generatePlan(left)
      generatePlan(right)
    case Relation(ns) =>
      outputNode(ns.name)
  }
}