package edu.berkeley.cs.scads.piql

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import edu.berkeley.cs.scads.piql.parser.{BoundValue, BoundIntegerValue, BoundStringValue, BoundFixedValue}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.generic.{IndexedRecord, GenericData}
import edu.berkeley.cs.scads.storage.Namespace

abstract sealed class JoinCondition
case class AttributeCondition(attrName: String) extends JoinCondition
case class BoundValueLiteralCondition[T](fieldValue: BoundFixedValue[T]) extends JoinCondition

case class EntityClass(name: String)

/* Query Plan Nodes */
abstract sealed class QueryPlan
abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: List[BoundValue]) extends TupleProvider
case class PrefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, conditions: Seq[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, conditions: Seq[JoinCondition], child: EntityProvider) extends TupleProvider
case class Materialize(entityType: EntityClass, child: TupleProvider) extends EntityProvider
case class Selection(equalityMap: HashMap[String, BoundValue], child: EntityProvider) extends EntityProvider
case class Sort(fields: List[String], ascending: Boolean, child: EntityProvider) extends EntityProvider
case class TopK(k: BoundValue, child: EntityProvider) extends EntityProvider

class Environment {
  var namespaces: Map[String, Namespace[SpecificRecordBase, SpecificRecordBase]] = null
}

abstract trait QueryExecutor {
	val qLogger = Logger.getLogger("scads.queryexecution")
	/* Type Definitions */
	type TupleStream = Seq[(SpecificRecordBase, SpecificRecordBase)]
	type EntityStream = Seq[Entity[_,_]]

  implicit def toBoundInt(i: Int) = BoundIntegerValue(i)
  implicit def toBoundString(s: String) = BoundStringValue(s)

	/* Tuple Providers */
	protected def singleGet(namespace: String, key: List[BoundValue])(implicit env: Environment): TupleStream = {
    val ns = env.namespaces(namespace)
    val keyRec = ns.keyClass.newInstance()
    key.zipWithIndex.foreach {
      case(v: BoundFixedValue[_], idx: Int) => keyRec.put(idx, v.value)
    }

    ns.get(keyRec) match {
      case Some(v) => List((keyRec, v))
      case None => List(null)
    }
  }

  //TODO: Use limit/ascending parameters
	protected def prefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean)(implicit env: Environment): TupleStream = {
    val ns = env.namespaces(namespace)
    val key = ns.keyClass.newInstance()
    prefix.zipWithIndex.foreach {
      case (value: BoundValue, idx: Int) => key.put(idx, value)
    }
    ns.getPrefix(key, prefix.length)
  }

  //TODO: Deal with values that return None
	protected def sequentialDereferenceIndex(targetNamespace: String, child: TupleStream)(implicit env: Environment): TupleStream = {
    val ns = env.namespaces(targetNamespace)
    child.map(c => (c._2, ns.get(c._2).get))
  }

  //TODO: use limit / ascending parameters
  //TODO: parallelize
	protected def prefixJoin(namespace: String, conditions: List[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityStream)(implicit env: Environment): TupleStream = {
    val ns = env.namespaces(namespace)
    child.flatMap(c => {
      val key = ns.keyClass.newInstance()
      conditions.zipWithIndex.foreach {
        case (cond: AttributeCondition, idx: Int) => key.put(idx, c.get(cond.attrName))
        case (cond: BoundValueLiteralCondition[_], idx: Int) => key.put(idx, cond.fieldValue.value)
      }
      ns.getPrefix(key, conditions.length)
    })
  }

	protected def pointerJoin(namespace: String, conditions: List[JoinCondition], child: EntityStream)(implicit env: Environment): TupleStream = {
    val ns = env.namespaces(namespace)
    child.map(c => {
      val key = ns.keyClass.newInstance()
      conditions.zipWithIndex.foreach {
        case (cond: AttributeCondition, idx: Int) => key.put(idx, c.get(cond.attrName))
        case (cond: BoundValueLiteralCondition[_], idx: Int) => key.put(idx, cond.fieldValue.value)
      }
      (key, ns.get(key).get)
    })
  }

	/* Entity Providers */
	protected def materialize(entityClass: Class[Entity[_,_]], child: TupleStream)(implicit env: Environment): EntityStream = {
    child.map(c => {
      val e = entityClass.newInstance().asInstanceOf[Entity[SpecificRecordBase,SpecificRecordBase]]
      e.key.parse(c._1.toBytes)
      e.value.parse(c._2.toBytes)
      e
    })
  }

	protected def selection(equalityMap: HashMap[String, BoundValue], child: EntityStream): EntityStream = {
    child.filter(c => {
      equalityMap.map {case (attrName: String, bv: BoundFixedValue[_]) => c.get(attrName) equals bv.value}.reduceLeft(_&_)
    })
  }

	protected def sort(fields: List[String], ascending: Boolean, child: EntityStream): EntityStream = {
    val comparator = (a: Entity[_,_], b: Entity[_,_]) => {
      fields.map(f => (a.get(f), b.get(f)) match {
        case (x: Integer, y: Integer) => x.intValue() < y.intValue()
        case (x: String, y: String) => x < y
      }).reduceLeft(_&_)
    }

    val ret = child.toArray
    scala.util.Sorting.stableSort(ret, comparator)
    if(ascending) ret else ret.reverse
  }

	protected def topK(k: BoundIntegerValue, child: EntityStream): EntityStream = child.take(k.value)
}
