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

  protected def outputCluster[A](label: String*)(func: => A)(implicit sb: StringBuilder, indnt: Indentation): A = {
    outputBraced("subGraph ", "cluster" + nextId) {
      output("label=", quote(label.mkString), ";")
      func
    }
  }

  protected def getPredicates(ns: String, keySpec: List[BoundValue], child: DotNode)(implicit sb: StringBuilder, indnt: Indentation): DotNode = {
    val keySchema = entities.find(_.namespace equals ns).get.keySchema
    keySpec.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: BoundValue, idx: Int)) => {
        val fieldName = keySchema.getFields.get(idx).name
        val selection = outputDotNode("selection\n" + fieldName + "=" + prettyPrint(value), shape="ellipse")
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

  case class SubPlan(graph: DotNode, recCount: Int)
  protected def generateGraph(plan: QueryPlan)(implicit sb: StringBuilder, indnt: Indentation): SubPlan = {
    plan match {
      case SingleGet(ns, key) => {
        val graph = outputCluster("SingleGet\n1 GET Operation") {
          getPredicates(ns, key, outputDotNode(ns))
        }
        SubPlan(graph, 1)
      }
      case PrefixGet(ns, prefix, limit, ascending) => {
        val graph = outputCluster("PrefixGet\n1 GET_RANGE Operation") {
          getPredicates(ns, prefix, outputDotNode(ns))
        }
        SubPlan(graph, getIntValue(limit))
      }
      case SequentialDereferenceIndex(ns, child) => {
        val childPlan = generateGraph(child)
        val graph = outputCluster("SequentialDeref\n", childPlan.recCount.toString, " GET Operations") {
          val targetNs = outputDotNode(ns)
          outputDotNode("Dereference", children=List(targetNs))
        }
        SubPlan(graph, childPlan.recCount)
      }
      case PrefixJoin(ns, conditions, limit, ascending, child) => {
        val childPlan = generateGraph(child)
        val graph = outputCluster("PrefixJoin\n", childPlan.recCount.toString, " GETRANGE Operations") {
          val source = outputDotNode(ns)
          val join = outputDotNode("Join", shape="diamond", children=List(childPlan.graph, source))
          val pred = getJoinPredicates(ns, conditions, join)
          limit match {
            case biv: BoundIntegerValue => outputEdge(pred, outputDotNode("DataStopAfter(" + biv.value + ")"))
            case v => outputEdge(pred, outputDotNode("StopAfter(" + prettyPrint(v) + ")"))
          }
        }
        SubPlan(graph, getIntValue(limit) * childPlan.recCount)
      }
      case PointerJoin(ns, conditions, child) => {
        val childPlan = generateGraph(child)
        val graph = outputCluster("PointerJoin\n", childPlan.recCount.toString, " GET Operations") {
          val source = outputDotNode(ns)
          val join = outputDotNode("Join", shape="diamond", children=List(childPlan.graph, source))
          getJoinPredicates(ns, conditions, join)
        }
        SubPlan(graph, childPlan.recCount)
      }
      case Materialize(entityType, child) => generateGraph(child)
      case Selection(equalityMap, child) => {
        val childPlan = generateGraph(child)
        val graph = equalityMap.map(p => p._1 + "=" + prettyPrint(p._2)).foldLeft(childPlan.graph) {
          case (child: DotNode, pred: String) =>
            outputDotNode("Selection\n" + pred, children=List(child), style="filled")
        }
        SubPlan(graph, childPlan.recCount)
      }
      case Sort(fields, ascending, child) => {
        val childPlan = generateGraph(child)
        val graph = outputDotNode("Sort " + fields.mkString("[", ",", "]"), children=List(childPlan.graph), style="filled")
        SubPlan(graph, childPlan.recCount)
      }
      case TopK(k, child) => {
        val childPlan = generateGraph(child)
        val graph = outputDotNode("TopK " + prettyPrint(k), children=List(childPlan.graph), style="filled")
        SubPlan(graph, getIntValue(k))
      }
    }
  }

  //TODO: Fix the types so this casting isn't needed
  protected def getIntValue(v: BoundValue): Int = v match {
    case biv: BoundIntegerValue => biv.value
    case _ => 1000
  }

  protected def prettyPrint(value: BoundValue): String = value match {
    case BoundTrueValue => "true"
    case BoundFalseValue => "false"
    case BoundParameter(name, _) => "[" + name + "]"
    case BoundThisAttribute(name, _) => "[this]." + name
    case BoundIntegerValue(i) => i.toString
  }

  protected def prettyPrint(cond: JoinCondition): String = cond match {
    case AttributeCondition(name) => "[child]." + name
    case BoundValueLiteralCondition(value) => value.value.toString
  }
}
