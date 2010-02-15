package edu.berkeley.cs.scads.piql

import scala.collection.mutable.HashMap
import org.apache.log4j.Logger

/* Query Plan Nodes */
abstract sealed class QueryPlan
/*abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: Field, policy: ReadPolicy) extends TupleProvider
case class PrefixGet(namespace: String, prefix: Field, limit: Field, ascending: Boolean, policy: ReadPolicy) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, conditions: List[JoinCondition], limit: Field, ascending: Boolean, policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, conditions: List[JoinCondition], policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class Materialize(entityClass: Class[Entity], child: TupleProvider) extends EntityProvider
case class Selection(equalityMap: HashMap[String, Field], child: EntityProvider) extends EntityProvider
case class Sort(fields: List[String], ascending: Boolean, child: EntityProvider) extends EntityProvider
case class TopK(k: Field, child: EntityProvider) extends EntityProvider
*/
