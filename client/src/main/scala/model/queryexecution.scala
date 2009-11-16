package edu.berkeley.cs.scads.model

import scala.collection.mutable.HashMap

abstract trait QueryExecutor {
	/* Type Definitions */
	type TupleStream = Seq[Tuple]
	type EntityStream = Seq[Entity]
	type LimitValue = Field

	/* Metadata Catalog */
	protected val nsKeys: Map[String, List[Class[Field]]]
	protected val nsVersions: Map[String, Boolean]

	/* Tuple Providers */
	protected def singleGet(namespace: String, key: Field, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
		List(policy.get(namespace, key.serializeKey, nsKeys(namespace), nsVersions(namespace)))
	}

	protected def prefixGet(namespace: String, prefix: Field, limit: LimitValue, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
		val serializedPrefix = prefix.serializeKey
		policy.get_set(namespace, serializedPrefix, serializedPrefix + "~", limitToInt(limit), nsKeys(namespace), nsVersions(namespace))
	}

	protected def sequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleStream)(implicit env: Environment): TupleStream = {
		child.map((t) => {
			policy.get(targetNamespace, t.value, nsKeys(targetNamespace), nsVersions(targetNamespace))
		})
	}

	protected def prefixJoin(namespace: String, attribute: String, limit: LimitValue, policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
		child.flatMap((e) => {
			val prefix = e.attributes(attribute).serializeKey
			policy.get_set(namespace, prefix, prefix + "~", limitToInt(limit), nsKeys(namespace), nsVersions(namespace))
		})
	}

	protected def pointerJoin(namespace: String, attributes: List[String], policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
		child.map((e) => {
				val key = attributes.map(e.attributes).map(_.serializeKey).mkString("", ", ", "")
				policy.get(namespace, key, nsKeys(namespace), nsVersions(namespace))
		})
	}

	/* Entity Providers */
	protected def materialize[EntityType <: Entity](entityClass: Class[EntityType], child: TupleStream)(implicit env: Environment): Seq[EntityType] = {
		child.map((t) => {
			val entity = entityClass.getConstructors()(0).newInstance(env).asInstanceOf[EntityType]
			entity.deserializeAttributes(t.value)
			entity
		})
	}

	protected def selection[EntityType <: Entity](equalityMap: HashMap[String, Field], child: Seq[EntityType]): Seq[EntityType] = {
		child.filter((e) => {
			equalityMap.foldLeft(true)((value: Boolean, equality: (String, Field)) => {
					value && (e.attributes(equality._1) == equality._2)
				})
		})
	}

	protected def sort[EntityType <: Entity](fields: List[String], child: Seq[EntityType]): Seq[EntityType] = {
		child.toList.sort((e1, e2) => {
			(fields.map(e1.attributes).map(_.serializeKey).mkString("", "", "") compare fields.map(e2.attributes).map(_.serializeKey).mkString("", "", "")) < 0
		})
	}

	/* Helper functions */
	private def limitToInt(lim: LimitValue): Int = lim match {
		case i: IntegerField => i.value
		case _ => throw new IllegalArgumentException("Only integerFields are accepted as limit parameters")
	}
}

/* Query Plan Nodes */
abstract sealed class QueryPlan
abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: Field, policy: ReadPolicy) extends TupleProvider
case class PrefixGet(namespace: String, prefix: Field, limit: Field, policy: ReadPolicy) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, attribute: String, limit: Field, policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, attributes: List[String], policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class Materialize(entityClass: Class[Entity], child: TupleProvider) extends EntityProvider
case class Selection(equalityMap: HashMap[String, Field], child: EntityProvider) extends EntityProvider
case class Sort(fields: List[String], child: EntityProvider) extends EntityProvider
