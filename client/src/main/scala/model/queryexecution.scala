package edu.berkeley.cs.scads.model

import java.text.ParsePosition
import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSet
import edu.berkeley.cs.scads.thrift.RangeSet
import edu.berkeley.cs.scads.keys.TransparentKey
import org.apache.log4j.Logger


case class Tuple(key: Field, version: Version, value: String)

abstract class ReadPolicy {
	def get(namespace: String, key: String, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): Tuple
	def get_set(namespace: String, startKey: String, endKey: String, limit: Int, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): List[Tuple]

	protected def makeTuple(rec: Record, keyClass: List[Class[Field]], versioned: Boolean): Tuple = {
		val key = CompositeField(keyClass)
		val version = if(versioned) new IntegerVersion else Unversioned
		val pos = new ParsePosition(0)
		key.deserializeKey(rec.key)
		version.deserialize(rec.value, pos)
		val value = rec.value.substring(pos.getIndex)
		Tuple(key, version, value)
	}
}

object ReadRandomPolicy extends ReadPolicy {
	val logger = Logger.getLogger("scads.readpolicy.readrandom")
	val rand = new scala.util.Random()

	def get(namespace: String, key: String, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): Tuple = {
		val nodes = env.placement.lookup(namespace, new TransparentKey(key))
		val node = nodes(rand.nextInt(nodes.length))

		val rec = node.useConnection((c) => {
			c.get(namespace, key)
		})
		makeTuple(rec, keyClass, versioned)
	}

	def get_set(namespace: String, startKey: String, endKey: String, limit: Int, keyClass: List[Class[Field]], versioned: Boolean)(implicit env: Environment): List[Tuple] = {
		val node = env.placement.lookup(namespace, new TransparentKey(startKey))

		val result = node(0).useConnection((c) => {
			val range = new RangeSet(startKey, endKey, 0, limit)
			val rset = new RecordSet()

			rset.setType(edu.berkeley.cs.scads.thrift.RecordSetType.RST_RANGE)
			rset.setRange(range)
			c.get_set(namespace, rset)
		})

		logger.debug("result: " + result)
		val tuples = new scala.collection.mutable.ListBuffer[Tuple]()
		var i = 0
		while(i < result.size) {
			logger.debug("Making a tuple from " + result.get(i) + " key: " + keyClass + " ver: " + versioned)
			tuples.append(makeTuple(result.get(i), keyClass, versioned))
			i += 1
		}
		tuples.toList
	}
}

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
