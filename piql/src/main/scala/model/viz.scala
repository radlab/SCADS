package edu.berkeley.cs.scads.piql

import java.io.File
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.piql.DynamicDispatch._
import edu.berkeley.cs.scads.storage.TestScalaEngine

case class DotGraph(name: String, components: Iterable[DotComponent])

object DotComponent {
  private val curId = new java.util.concurrent.atomic.AtomicInteger
  def nextId = curId.getAndIncrement()
}

abstract class DotComponent {val id: String}

case class SubGraph(label: String, nodes: List[DotComponent]) extends DotComponent {
  val graphId = "cluster" + DotComponent.nextId
  val id = nodes.head.id
}

case class DotNode(label: String, children: List[DotComponent] = Nil, shape: String = "box", fillcolor: String = "azure3", style: String = "none") extends DotComponent {
  val id = "dotNode" + DotComponent.nextId
  def this(label: String, child: DotNode) = this(label, List(child))
}

object GraphVis {
  def main(args: Array[String]): Unit = {
    val piql = Compiler.readFile(new File(args(0)))
    val ast = Compiler.getAST(piql)
    val boundAst = new Binder(ast).bind
    val spec = new Optimizer(boundAst).optimizedSpec
    val entities = spec.entities.map(_._2).toList
    val orphanPlans = spec.orphanQueries.map(q => SubGraph(q._1, List(generateGraph(q._2.plan, entities))))
    val instancePlans = spec.entities.flatMap(e => {
      e._2.queries.map(q => SubGraph(e._1 + "." + q._1, List(generateGraph(q._2.plan, entities))))
    })

    val outfile = new java.io.FileWriter("plans.dot")
    val dot = DotGen(DotGraph("QueryPlans", orphanPlans ++ instancePlans))
    outfile.write(dot)
    outfile.close()
    System.exit(0)
  }

  protected def getPlans(spec: BoundSpec): Iterable[QueryPlan] = spec.orphanQueries.map(_._2).map(_.plan) ++ spec.entities.map(_._2).flatMap(_.queries).map(_._2).map(_.plan)

  def generateGraph(plan: QueryPlan, entities: List[BoundEntity]): DotComponent = {

    def getPredicates(ns: String, keySpec: List[BoundValue], child: DotNode): DotNode = {
      val keySchema = entities.find(_.namespace equals ns).get.keySchema
      keySpec.zipWithIndex.foldLeft(child) {
        case (subPlan: DotNode, (value: BoundValue, idx: Int)) => {
          val fieldName = keySchema.getFields.get(idx).name
          DotNode("selection " + fieldName + "=" + value, List(subPlan), shape="ellipse")
        }
      }
    }

    def getJoinPredicates(ns: String, conditions: Seq[JoinCondition], child: DotNode): DotNode = {
      val keySchema = entities.find(_.namespace equals ns).get.keySchema
      conditions.zipWithIndex.foldLeft(child) {
        case (subPlan: DotNode, (value: JoinCondition, idx: Int)) => {
          val fieldName = keySchema.getFields.get(idx).name
          DotNode("selection " + fieldName + "=" + value, List(subPlan), shape="ellipse")
        }
      }
    }

    plan match {
      case SingleGet(ns, key) => SubGraph("SingleGet", List(getPredicates(ns, key, DotNode(ns))))
      case PrefixGet(ns, prefix, limit, ascending) => SubGraph("PrefixGet", List(getPredicates(ns, prefix, DotNode(ns))))
      case SequentialDereferenceIndex(ns, child) => SubGraph("SequentialDeref", List(DotNode("deref", List(DotNode(ns), generateGraph(child, entities)))))
      case PrefixJoin(ns, conditions, limit, ascending, child) => SubGraph("PrefixJoin", List(getJoinPredicates(ns, conditions, DotNode("join", List(DotNode(ns), generateGraph(child, entities)), shape="diamond"))))
      case PointerJoin(ns, conditions, child) => SubGraph("PointerJoin", List(getJoinPredicates(ns, conditions, DotNode("join", List(DotNode(ns), generateGraph(child, entities)), shape="diamond"))))
      case Materialize(entityType, child) => generateGraph(child, entities)
      case Selection(equalityMap, child) => DotNode("selection", List(generateGraph(child, entities)), style="filled")
      case Sort(fields, ascending, child) => DotNode("sort", List(generateGraph(child, entities)), style="filled")
      case TopK(k, child) => DotNode("topk " + k, List(generateGraph(child, entities)), style="filled")
    }
  }
}

object DotGen extends Generator[DotGraph] {
  private val curId = new java.util.concurrent.atomic.AtomicInteger

  protected def generate(graph: DotGraph)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    outputBraced("digraph ", graph.name) {
      output("rankdir=BT;")
      graph.components.foreach(generateSubGraph)
    }
  }

  protected def generateSubGraph(graph: DotComponent)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    graph match {
      case node: DotNode => {
        output(node.id, "[label=", quote(node.label), ", shape=", node.shape, ", fillcolor=", node.fillcolor, ", style=", node.style, "];")
        node.children.foreach(generateSubGraph)
        node.children.map(_.id).foreach(id => output(id, " -> ", node.id, ";"))
      }
      case subGraph: SubGraph => {
        outputBraced("subgraph ", subGraph.graphId) {
          output("label=", quote(subGraph.label), ";")
          subGraph.nodes.foreach(generateSubGraph)
        }
      }
    }
  }
}
