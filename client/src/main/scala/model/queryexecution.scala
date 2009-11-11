package edu.berkeley.cs.scads.model

abstract trait QueryExecutor {
	/* Return Types */
	type TupleStream = Seq[Tuple]
	type EntityStream = Seq[Entity]

	/* Metadata Catalog */
	protected val nsKeys: Map[String, List[Class[Field]]]
	protected val nsVersions: Map[String, Boolean]

	/* Tuple Providers */
	protected def singleGet(namespace: String, key: Field, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
		List(policy.get(namespace, key.serializeKey, nsKeys(namespace), nsVersions(namespace)))
	}

	protected def prefixGet(namespace: String, prefix: Field, limit: Int, policy: ReadPolicy)(implicit env: Environment): TupleStream = {
		val serializedPrefix = prefix.serializeKey
		policy.get_set(namespace, serializedPrefix, serializedPrefix + "~", limit, nsKeys(namespace), nsVersions(namespace))
	}

	protected def sequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleStream)(implicit env: Environment): TupleStream = {
		child.map((t) => {
			policy.get(targetNamespace, t.value, nsKeys(targetNamespace), nsVersions(targetNamespace))
		})
	}

	protected def prefixJoin(namespace: String, attribute: String, limit: Int, policy: ReadPolicy, child: EntityStream)(implicit env: Environment): TupleStream = {
		child.flatMap((e) => {
			val prefix = e.attributes(attribute).serializeKey
			policy.get_set(namespace, prefix, prefix + "~", limit, nsKeys(namespace), nsVersions(namespace))
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
}

/* Query Plan Nodes */
abstract sealed class QueryPlan
abstract class TupleProvider extends QueryPlan
abstract class EntityProvider extends QueryPlan
case class SingleGet(namespace: String, key: Field, policy: ReadPolicy) extends TupleProvider
case class PrefixGet(namespace: String, prefix: Field, limit: Int, policy: ReadPolicy) extends TupleProvider
case class SequentialDereferenceIndex(targetNamespace: String, policy: ReadPolicy, child: TupleProvider) extends TupleProvider
case class PrefixJoin(namespace: String, attribute: String, limit: Int, policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class PointerJoin(namespace: String, attributes: List[String], policy: ReadPolicy, child: EntityProvider) extends TupleProvider
case class Materialize(entityClass: Class[Entity], child: TupleProvider) extends EntityProvider
