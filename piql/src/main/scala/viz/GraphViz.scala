package edu.berkeley.cs.scads.piql
package viz

import java.io.File
import plans._
import opt._


object Test {

  import edu.berkeley.cs.scads.piql.test.Relations._

  val query = (
    r2.where("r2.f1".a === (0.?))
      .dataLimit(5)
      .join(r2Prime)
      .where("r2.f2".a === "r2Prime.f1".a)
      .sort("r2Prime.f2".a :: Nil)
      .paginate(10)
    )

  def deltas = new QueryDeltas(new Qualifier(query).qualifiedPlan)

  val boundQuery = (
    r2.where(QualifiedAttributeValue(Relation(r2), r2.schema.getField("f1")) === (0.?))
      .dataLimit(5)
      .join(r2Prime)
      .where(QualifiedAttributeValue(Relation(r2), r2.schema.getField("f2")) === QualifiedAttributeValue(Relation(r2Prime), r2Prime.schema.getField("f1")))
      .sort(QualifiedAttributeValue(Relation(r2Prime), r2Prime.schema.getField("f2")) :: Nil)
      .paginate(10)
    )

  val deltaR2 = (
    LocalTuples(0, "@r2", r2.keySchema, r2.schema)
      .dataLimit(1)
      .join(r2Prime)
      .where("@r2.f2".a === "r2Prime.f1".a)
      .sort("r2Prime.f2".a :: Nil)
      .select("@r2.f1", "r2Prime.f2", "r2Prime.f1")
      .paginate(10)
    )

  val deltaR2Prime = (
    LocalTuples(0, "@r2Prime", r2Prime.keySchema, r2.schema)
      .dataLimit(1)
      .join(r2)
      .where("@r2Prime.f1".a === "r2.f2".a)
      .select("r2.f1", "@r2Prime.f2", "@r2Prime.f1")
    )

  def main(args: Array[String]): Unit = {
    query.graphViz
    sys.exit()
  }
}

trait GraphViz extends FileGenerator {
  protected val svgFile = File.createTempFile("queryPlan", ".svg")
  private val curId = new java.util.concurrent.atomic.AtomicInteger

  protected def nextId = curId.getAndIncrement()

  abstract override def finish(): File = {
    val dotFile = super.finish()
    Runtime.getRuntime.exec(Array("dot", "-o" + svgFile, "-Tsvg", dotFile.getCanonicalPath))
    svgFile
  }

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
    output(node.id, "[label=", quote(label), ", shape=", shape, ", fillcolor=", fillcolor, ", style=", style, ", fontsize=" + fontSize + "];")
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


protected class GraphLogicalPlan extends GraphViz {
  lazy val file = File.createTempFile("queryPlan", ".dot")

  /* Need a function with return type Unit for Generator */
  def generate(plan: LogicalPlan): Unit = {
    digraph("QueryPlan") {
      generatePlan(plan)
    }
  }

  val joinSym = "&#9285;"
  val selectionSym = "&#963;"
  val projectSym = "&#960;"

  /* Recursive function to draw individual plan nodes */
  protected def generatePlan(plan: LogicalPlan): DotNode = plan match {
    case Project(values, child) =>
      outputNode(projectSym + " " + values.map(prettyPrint).mkString(", "),
        children = Seq(generatePlan(child)))
    case Selection(predicate, child) =>
      outputNode(selectionSym + " " + prettyPrint(predicate),
        children = Seq(generatePlan(child)))
    case Sort(attributes, ascending, child) =>
      outputNode("sort " + (if (ascending) "asc" else "desc") + "\n" + attributes.map(prettyPrint).mkString(", "),
        children = Seq(generatePlan(child)))
    case StopAfter(count, child) =>
      outputNode("stop\n" + prettyPrint(count),
        children = Seq(generatePlan(child)))
    case DataStopAfter(count, child) =>
      outputNode("data stop\n" + prettyPrint(count),
        children = Seq(generatePlan(child)))
    case Paginate(count, child) =>
      outputNode("paginate\n" + prettyPrint(count),
        children = Seq(generatePlan(child)))
    case Join(left, right) =>
      outputNode(joinSym,
        fontSize = 36,
        children = Seq(generatePlan(left), generatePlan(right)))
    case Relation(ns, alias) =>
      outputNode(ns.name + alias.map(a => " " + a).getOrElse(""),
        shape = "box")
    case LocalTuples(_, alias, _, _) =>
      outputNode(alias, shape = "box")
  }

  protected def prettyPrint(p: Predicate): String = p match {
    case EqualityPredicate(v1, v2) => prettyPrint(v1) + " = " + prettyPrint(v2)
  }

  protected def prettyPrint(v: Value): String = v match {
    case UnboundAttributeValue(name) => name
    case QualifiedAttributeValue(r,f) => r.name + "." + f.name
    case ConstantValue(c) => c.toString
    case ParameterValue(n) => "[" + n.toString + "]"
  }

  protected def prettyPrint(v: Limit): String = v match {
    case FixedLimit(n) => n.toString
    case ParameterLimit(n, max) => "[" + n.toString + "]"
  }
}