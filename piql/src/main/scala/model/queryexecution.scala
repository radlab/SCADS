package edu.berkeley.cs.scads.piql

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger
import edu.berkeley.cs.scads.piql.parser.BoundValue
import org.apache.avro.generic.IndexedRecord

abstract sealed class JoinCondition
case class AttributeCondition(attrName: String) extends JoinCondition
case class FieldLiteralCondition(fieldValue: BoundValue) extends JoinCondition

/* Query Plan Nodes */
abstract sealed class QueryPlan
abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: List[BoundValue]) extends TupleProvider
case class PrefixGet(namespace: String, prefix: List[BoundValue], limit: BoundValue, ascending: Boolean) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, conditions: Seq[JoinCondition], limit: BoundValue, ascending: Boolean, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, conditions: Seq[JoinCondition], child: EntityProvider) extends TupleProvider
case class Materialize(entityType: String, child: TupleProvider) extends EntityProvider
case class Selection(equalityMap: HashMap[String, BoundValue], child: EntityProvider) extends EntityProvider
case class Sort(fields: List[String], ascending: Boolean, child: EntityProvider) extends EntityProvider
case class TopK(k: BoundValue, child: EntityProvider) extends EntityProvider

