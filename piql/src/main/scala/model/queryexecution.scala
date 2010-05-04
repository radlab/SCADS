package edu.berkeley.cs.scads.piql

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import edu.berkeley.cs.scads.piql.parser.{BoundValue, BoundIntegerValue, BoundStringValue, BoundFixedValue}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.avro.generic.{IndexedRecord, GenericData}
import edu.berkeley.cs.scads.storage.Namespace

abstract sealed class JoinCondition
case class AttributeCondition(attrName: String) extends JoinCondition
case class BoundValueLiteralCondition(fieldValue: BoundValue) extends JoinCondition

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
    val keyRec = ns.keyClass.newInstance().asInstanceOf[SpecificRecordBase]
    key.zipWithIndex.foreach {
      case(v: BoundFixedValue[_], idx: Int) => keyRec.put(idx, v.value)
    }

    ns.get(keyRec) match {
      case Some(v) => List((keyRec, v))
      case None => List(null)
    }
  }

	protected def prefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean)(implicit env: Environment): TupleStream = null

	protected def sequentialDereferenceIndex(targetNamespace: String, child: TupleStream)(implicit env: Environment): TupleStream = null

	protected def prefixJoin(namespace: String, conditions: List[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityStream)(implicit env: Environment): TupleStream = null

	protected def pointerJoin(namespace: String, conditions: List[JoinCondition], child: EntityStream)(implicit env: Environment): TupleStream = null

	/* Entity Providers */
	protected def materialize(entityClass: Class[Entity[_,_]], child: TupleStream)(implicit env: Environment): EntityStream = {
    child.map(c => {
      val e = entityClass.newInstance().asInstanceOf[Entity[SpecificRecordBase,SpecificRecordBase]]
      e.key.parse(c._1.toBytes)
      e.value.parse(c._2.toBytes)
      e
    })
  }

	protected def selection(equalityMap: HashMap[String, BoundValue], child: EntityStream): EntityStream = null

	protected def sort(fields: List[String], ascending: Boolean, child: EntityStream): EntityStream = null

	protected def topK(k: BoundIntegerValue, child: EntityStream): EntityStream = child.take(k.value)
}
