package edu.berkeley.cs.scads.piql

import java.io.File
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.piql.DynamicDispatch._
import edu.berkeley.cs.scads.storage.TestScalaEngine

object DotNode {
  private val curId = new java.util.concurrent.atomic.AtomicInteger
  def nextId: String = "dotNode" + curId.getAndIncrement()
}
case class DotNode(label: String, children: List[DotNode] = Nil) {
  val id = DotNode.nextId
  def this(label: String, child: DotNode) = this(label, List(child))
}

object GraphVis {
  def main(args: Array[String]): Unit = {
    val piql = Compiler.readFile(new File(args(0)))
    val ast = Compiler.getAST(piql)
    val boundAst = new Binder(ast).bind
    val opt = new Optimizer(boundAst).optimizedSpec
    val code = ScalaGen(opt)
    val compiler = new ScalaCompiler
    compiler.compile(code)
    val configurator = compiler.classLoader.loadClass("piql.Configurator")

    implicit val env: Environment = configurator.callStatic("configure", List(TestScalaEngine.cluster)) match {
      case e: Environment => e
      case err => throw new RuntimeException("Bad configurator return type: " + err.getClass)
    }

    getPlans(opt).map(generateGraph).foreach(g => println(DotGen(g)))
  }

  protected def getPlans(spec: BoundSpec): Iterable[QueryPlan] = spec.orphanQueries.map(_._2).map(_.plan) ++ spec.entities.map(_._2).flatMap(_.queries).map(_._2).map(_.plan)

  protected def getPredicates(ns: String, keySpec: List[BoundValue], child: DotNode)(implicit env: Environment): DotNode = {
    val namespace = env.namespaces(ns)

    keySpec.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: BoundValue, idx: Int)) => {
        val fieldName = namespace.keySchema.getFields.get(idx).name
        DotNode("selection " + fieldName + "=" + value, List(subPlan))
      }
    }
  }

  def generateGraph(plan: QueryPlan)(implicit env: Environment): DotNode = {
    plan match {
      case SingleGet(ns, key) => getPredicates(ns, key, DotNode(ns))
      case PrefixGet(ns, prefix, limit, ascending) => getPredicates(ns, prefix, DotNode(ns))
      case SequentialDereferenceIndex(ns, child) => DotNode("deref", List(DotNode(ns), generateGraph(child)))
      case PrefixJoin(ns, conditions, limit, ascending, child) => DotNode("join", List(DotNode(ns), generateGraph(child)))
      case PointerJoin(ns, conditions, child) => DotNode("join", List(DotNode(ns), generateGraph(child)))
      case Materialize(entityType, child) => generateGraph(child)
      case Selection(equalityMap, child) => DotNode("selection", List(generateGraph(child)))
      case Sort(fields, ascending, child) => DotNode("sort", List(generateGraph(child)))
      case TopK(k, child) => DotNode("topk " + k, List(generateGraph(child)))
    }
  }
}

object DotGen extends Generator[DotNode] {
  private val curId = new java.util.concurrent.atomic.AtomicInteger

  protected def generate(graph: DotNode)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    outputBraced("digraph Plan" + curId.getAndIncrement()) {
      def outputNode(node: DotNode): Unit = output(node.id, "[label=", quote(node.label), "];")
      def outputEdge(node1: DotNode, node2: DotNode):Unit = output(node1.id, " -> ", node2.id, ";")
      traverse(graph, outputNode, outputEdge)
    }
  }

  private def traverse(graph: DotNode, nodeFunc: Function1[DotNode, Unit], edgeFunc: Function2[DotNode, DotNode, Unit]): Unit = {
    nodeFunc(graph)
    graph.children.foreach(traverse(_, nodeFunc, edgeFunc))
    graph.children.foreach(edgeFunc(graph, _))
  }
}
