package edu.berkeley.cs.scads.model

import java.text.ParsePosition
import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSet
import edu.berkeley.cs.scads.thrift.RangeSet

import edu.berkeley.cs.scads.keys.TransparentKey
import org.apache.log4j.Logger

object Util {
	val rand = new scala.util.Random()
}

/**
 * Abstract definition of a trait that allows you to get single values from the storage layer in the environment.
 * Concrete versions of this trait will be mixed in with execution operators that need to do single gets based on their designed perf/consistency.
 * TODO: concrete implementations of ReadRepair, Quorum, Etc.
 */
abstract trait Getter {
	val logger: Logger

	/**
	 * Get a single tuple from the storage engine.
	 * @param namespace the namespace to look in
	 * @param key the key represented as a field that will be looked up
	 * @param version a version class that will be used to deserialize the version from this record (if it exists)
	 * @returns either a (key, version, value) tuple or (key, null, null) if the key doesn't exist
	 */
	def get(namespace: String, key: Field, version: Version)(implicit env: Environment): (Field, Version, String)
  def getterType: String
}

/*
 * Abstract definition of a trait that allows you to get ranges of values from the storage layer in the environment.
 * Concrete versions of this trait will be mixed in with execution operators that need to do single gets based on their designed perf/consistency.
 * TODO: concrete implementations of ReadOne, ReadRepair, Quorum, Etc.
 */
abstract trait SetGetter {
	val logger: Logger

	/**
	 * Get a range of tuples from the storage engine.
	 */
	def get_set(namespace: String, start_key: String, end_key: String, limit: Int, keyType: Field, version: Version)(implicit env: Environment): Seq[(Field, Version, String)]
}

trait ReadOneSetGetter extends SetGetter{
	/* FIXME: to work with multiple nodes */
	def get_set(namespace: String, start_key: String, end_key: String, limit: Int, keyType: Field, version: Version)(implicit env: Environment): Seq[(Field, Version, String)] = {
		val node = env.placement.lookup(namespace, new TransparentKey(start_key))

		val result = scala.collection.jcl.Conversions.convertList(node(0).useConnection((c) => {
			val range = new RangeSet(start_key, end_key, 0, limit)
			val rset = new RecordSet()

			rset.setType(edu.berkeley.cs.scads.thrift.RecordSetType.RST_RANGE)
			rset.setRange(range)
			logger.debug("Doing get_set on ns: " + namespace + " recset: " + rset)
			c.get_set(namespace, rset)
		}))

		logger.debug("Results: " + result)

		result.map((rec) => {
			val key = keyType.duplicate()
			key.deserialize(rec.key)
			val pos = new ParsePosition(0)
			val ver = version.duplicate()
			ver.deserialize(rec.value, pos)
			(key, ver, rec.value.substring(pos.getIndex()))
		})
	}
}

/**
 * A concrete getter that reads randomly from one of the storage engines that is responsible for the specified key
 */
trait ReadOneGetter extends Getter {
	def get(namespace: String, key: Field, version: Version)(implicit env: Environment): (Field, Version, String) = {
		val sk = key.serializeKey
		val nodes = env.placement.lookup(namespace, new TransparentKey(sk))
		val pick = Util.rand.nextInt(nodes.length)

		nodes(pick).useConnection((c) => {
			val rec = c.get(namespace, sk)
			if(rec.value != null) {
				val ver = version.duplicate()
				val pos = new ParsePosition(0)
				ver.deserialize(rec.value, pos)

				logger.debug(" Read " + rec + " from " + namespace + " on " + nodes(pick))
				(key, ver, rec.value.substring(pos.getIndex()))
			}
			else {
				(key, null, null)
			}

		})
	}

  def getterType(): String = "ReadOneGetter"
}

/**
 * A stackable trait that merges results from another get with that in the session store to provide users with guaranteed reading of their own writes.
 */
trait ReadOwnWrites extends Getter {

	override abstract def get(namespace: String, key: Field, version: Version)(implicit env: Environment): (Field, Version, String) = {
		val storeResult = super.get(namespace, key, version)
		val sessionResult = env.session.get(namespace, key)

		val result = if(storeResult._2 == null && sessionResult._2 == null)
			(key, null, null)
		else if (storeResult._2 == null && sessionResult._2 != null)
			sessionResult
		else if(storeResult._2 != null && sessionResult._2 == null)
			storeResult
		else if(sessionResult._2 > storeResult._2)
			sessionResult
		else
			storeResult

		logger.debug("ReadOwnWrite compare store<" + storeResult + "> and session<" + sessionResult + "> picked <" + result + ">")

		return result
	}
}

/**
 * Base class of all nodes that perform interactive query execution
 */
sealed abstract class ExecutionNode {
	val logger = Logger.getLogger("scads.executionNode")
}

/**
 * Base class for all execution nodes that provide (Key, Version, Value) Tuples to their parent.
 */
abstract class TupleProvider extends ExecutionNode {
	def exec(implicit env: Environment): Seq[(Field, Version, String)]
}

/**
 * Base class for all execution nodes that provide materialized entities to their parent.
 */
abstract class EntityProvider extends ExecutionNode {
	def exec(implicit env: Environment): Seq[Entity]
}

/**
 * Execution node that performs a lookup of a single key and returns a value
 * TODO: Exception when key doesn't exist?
 */
abstract case class SingleGet(namespace: String, key: Field, ver: Version) extends TupleProvider with Getter {
	def exec(implicit env: Environment): Seq[(Field, Version, String)] = {
    logger.debug(this)
		Array(get(namespace, key, ver))
	}

  override def toString(): String = "SingleGet(" + namespace + ", " + key + ", " + ver + ")"
}

/**
 * Take a list of tuples, often from an index, where the value is a key for target record another namespace.
 * @param targetNamespace the namespace that the keys in the value refer to
 * @param targetkeyType a field that can be used to deserialize the keys dereferenced tuple
 * @param targetVersion the versioning system that is used by the target value
 * @param child the stream of index tuples
 */
abstract case class SequentialDereferenceIndex(targetNamespace: String, targetKeyType: Field, targetVersion: Version, child: TupleProvider) extends TupleProvider with Getter {
	def exec(implicit env: Environment): Seq[(Field, Version, String)] = {
    logger.debug(this)
		child.exec.map((r) => {
			val key = targetKeyType.duplicate
			key.deserialize(r._3)
			get(targetNamespace, key, targetVersion)
		})
	}

  override def toString(): String = "SequentialDerefIndex(" + targetNamespace + ", " + targetKeyType + ", " + targetVersion + ")"
}

/**
 * Operater to grab ranges of values from the storage layer in the environment.
 * TODO: implement it!
 */
abstract case class PrefixGet(namespace: String, prefix: Field, limit: Int, keyType: Field, versionType: Version) extends TupleProvider with SetGetter {
	def exec(implicit env: Environment): Seq[(Field, Version, String)] = {
		get_set(namespace, prefix.serializeKey, prefix.serializeKey + "~", limit, keyType, versionType)
	}
}

abstract case class PrefixJoin(namespace: String, attribute: String, limit: Int, keyType: Field, versionType: Version, child: EntityProvider) extends TupleProvider with SetGetter {
	def exec(implicit env: Environment): Seq[(Field, Version, String)] = {
		child.exec.flatMap((e) => {
				val prefix = e.attributes(attribute).serializeKey
				get_set(namespace, prefix, prefix + "~", limit, keyType, versionType)
		})
	}
}

/**
 * Takes a tuple that represents an entity of type <code>Type</code> and materialized it into an actual class.
 * This is done by first instanciating a new instance of the entity and then deserializing all the fields in it.
 */
case class Materialize[Type <: Entity](child: TupleProvider)(implicit manifest : scala.reflect.Manifest[Type]) extends EntityProvider {
  lazy val entityType = manifest.erasure.getName
	val con = manifest.erasure.getConstructors()(0)

	def exec(implicit env: Environment): Seq[Type] = {
		child.exec.map(t => {
			val ent:Type = con.newInstance(env).asInstanceOf[Type]
			ent.deserializeAttributes(t._3)
			ent
		})
	}
}
