package edu.berkeley.cs.scads.piql

import java.io.File
import edu.berkeley.cs.scads.piql.parser._

object GraphViz {
  def main(args: Array[String]): Unit = {
    val piql = Compiler.readFile(new File(args(0)))
    val ast = Compiler.getAST(piql)
    val boundAst = new Binder(ast).bind
    val spec = new Optimizer(boundAst).optimizedSpec
    val entities = spec.entities.map(_._2).toList
    val orphanQueries = spec.orphanQueries
    val instanceQueries = spec.entities.flatMap(_._2.queries)
    val grapher = new GraphVis(spec.entities.map(_._2).toList)

    (orphanQueries ++ instanceQueries).foreach(q => {
      val outfile = new java.io.FileWriter(q._1 + ".dot")
      outfile.write(grapher(q._2.plan))
      outfile.close()
    })
    System.exit(0)
  }
}

class GraphVis(entities: List[BoundEntity]) extends Generator[QueryPlan] {
  private val curId = new java.util.concurrent.atomic.AtomicInteger
  protected def nextId = curId.getAndIncrement()

  protected def generate(plan: QueryPlan)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    outputBraced("digraph QueryPlan") {
      output("rankdir=BT;")
      generateGraph(plan)
    }
  }

  case class DotNode(id: String)
  protected def outputDotNode(label: String, shape: String = "box", fillcolor: String = "azure3", style: String = "none", children:List[DotNode]= Nil)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    val node = DotNode("node" + nextId)
    output(node.id, "[label=", quote(label), ", shape=", shape, ", fillcolor=", fillcolor, ", style=", style, "];")
    children.foreach(outputEdge(_, node))
    return node
  }

  protected def outputEdge(src: DotNode, dest: DotNode)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    output(src.id, " -> ", dest.id, ";")
    return dest
  }

  protected def outputCluster[A](label: String)(func: => A)(implicit sb: StringBuilder, indnt: Indentation): A = {
    outputBraced("subGraph ", "cluster" + nextId) {
      output("label=", quote(label), ";")
      func
    }
  }

  protected def getPredicates(ns: String, keySpec: List[BoundValue], child: DotNode)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    val keySchema = entities.find(_.namespace equals ns).get.keySchema
    keySpec.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: BoundValue, idx: Int)) => {
        val fieldName = keySchema.getFields.get(idx).name
        val selection = outputDotNode("selection\n" + fieldName + "=" + value, shape="ellipse")
        outputEdge(subPlan, selection)
      }
    }
  }


  protected def getJoinPredicates(ns: String, conditions: Seq[JoinCondition], child: DotNode)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    val keySchema = entities.find(_.namespace equals ns).get.keySchema
    conditions.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: JoinCondition, idx: Int)) => {
        val fieldName = keySchema.getFields.get(idx).name
        val selection = outputDotNode("selection\n" + fieldName + "=" + prettyPrint(value), shape="ellipse")
        outputEdge(subPlan, selection)
      }
    }
  }

  protected def generateGraph(plan: QueryPlan)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    plan match {
      case SingleGet(ns, key) => {
        outputCluster("SingleGet") {
          getPredicates(ns, key, outputDotNode(ns))
        }
      }
      case PrefixGet(ns, prefix, limit, ascending) => {
        outputCluster("PrefixGet") {
          getPredicates(ns, prefix, outputDotNode(ns))
        }
      }
      case SequentialDereferenceIndex(ns, child) => {
        val childGraph = generateGraph(child)
        outputCluster("SequentialDeref") {
          val targetNs = outputDotNode(ns)
          outputDotNode("Dereference", children=List(targetNs, childGraph))
        }
      }
      case PrefixJoin(ns, conditions, limit, ascending, child) => {
        val childGraph = generateGraph(child)
        outputCluster("PrefixJoin") {
          val source = outputDotNode(ns)
          val join = outputDotNode("Join", shape="diamond", children=List(childGraph, source))
          getJoinPredicates(ns, conditions, join)
        }
      }
      case PointerJoin(ns, conditions, child) => {
        val childGraph = generateGraph(child)
        outputCluster("PointerJoin") {
          val source = outputDotNode(ns)
          val join = outputDotNode("Join", shape="diamond", children=List(childGraph, source))
          getJoinPredicates(ns, conditions, join)
        }
      }
      case Materialize(entityType, child) => generateGraph(child)
      case Selection(equalityMap, child) => outputDotNode("selection", children=List(generateGraph(child)), style="filled")
      case Sort(fields, ascending, child) => outputDotNode("sort", children=List(generateGraph(child)), style="filled")
      case TopK(k, child) => outputDotNode("topk " + k, children=List(generateGraph(child)), style="filled")
    }
  }

  protected def prettyPrint(cond: JoinCondition): String = cond match {
    case AttributeCondition(name) => "[child]." + name
    case BoundValueLiteralCondition(value) => value.value.toString
  }
}
