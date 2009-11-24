package edu.berkeley.cs.scads.model

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.thrift.{ExistingValue, Record, TestAndSetFailure, StorageNode}

class ExecutionFailure extends Exception
case class OptimisticConcurrencyAbort(expected: ExistingValue, actual: Record) extends ExecutionFailure
case class PartialWrite(success: List[StorageNode], failure: List[StorageNode]) extends ExecutionFailure

/**
 * Abstract definition of an Action that can be performed in an async/reliable fashion
 * Intended to be used for updating indexes, etc.
 */
abstract class Action {
	val logger = Logger.getLogger("scads.Action")

	/**
	 * Actually perform the action on the datastore provided in the environment.
	 * Actions performed by run should attempt to be idempotent as this may be called more than one time if there is a failure either with the system or the action.
	 */
	def run(implicit env: Environment)

	/**
	 * Provide a sequence of tuples (namespace, key, version, value) representing the keys that will be written to the datastore when this action is run.
	 * This is often placed in the session store so that clients can "read their own writes", etc.
	 */
	def results(): Seq[(String, Field, Version, String)]
}

/**
 * Write to all nodes that are currently responsible for the key, using a put
 * TODO: throw the Partial write exception on failure
 */
class WriteAll(namespace: String, key: Field, value: String) extends Action {
	def run(implicit env: Environment) = {
		val rec = new Record(key.serializeKey, value)

		env.placement.locate(namespace, rec.key).foreach((n) => {
			n.useConnection((c) => {
				logger.debug("Putting " + rec + " to " + namespace + " on " + n)
				c.put(namespace, rec)
			})
		})
	}

	def results(): Seq[(String, Field, Version, String)] = {
		Array((namespace, key, Unversioned, value))
	}
}

/**
 * Write to all nodes that are currently responsible for the key using
 */
class VersionedWriteAll(namespace: String, key: Field, expectedVersion: Version, value: String) extends Action {
	def run(implicit env: Environment) = {
		val nVersion = expectedVersion.next()
		val rec = new Record(key.serializeKey, nVersion.serialize + value)
		val ev = new ExistingValue(expectedVersion.serialize, expectedVersion.prefixLength)

		env.placement.locate(namespace,rec.key).foreach((n) => {
			n.useConnection((c) => {
				logger.debug("Attempting to test/set " + rec + " to " + namespace + " on " + n + " with existing version " + ev)
				try {
					c.test_and_set(namespace, rec, ev)
				}
				catch {
					case tsf: TestAndSetFailure => {
						val currentValue = c.get("namespace", key.serializeKey)
						logger.warn("Optimistic concurrency abort, expected: " + ev + ", got: " + currentValue)
						throw OptimisticConcurrencyAbort(ev, currentValue)
					}
				}
			})
		})
	}

	def results(): Seq[(String, Field, Version, String)] = {
		Array((namespace, key, expectedVersion.next, value))
	}
}

/**
 * Abstract definition of a class that processes a list of actions both sync and async.
 */
abstract class Executor {
	val logger = Logger.getLogger("scads.executor")

	def execute(async: Seq[Action])
	def execute(sync: Action, async: Seq[Action])
}

/**
 * Executor that just does all actions in sync.
 * Intended for use only in testcases.
 */
class TrivialExecutor(implicit env: Environment) extends Executor {
	def execute(async: Seq[Action]) = async.foreach((a) => {logger.debug("Execing" + a); a.run})

	def execute(sync: Action, async: Seq[Action]) = {
		sync.run
		execute(async)
	}
}

/**
 * Executor that performs all sync actions and then 'looses' all async ones.
 * Intended for use in testcases where slow async actions need to be simulated.
 * TODO: Method that allows all 'lost' actions to be executed eventually.
 */
class LazyExecutor(implicit env: Environment) extends Executor {
	def execute(async: Seq[Action]) = async.foreach((a) => env.session.addWrites(a.results()))

	def execute(sync: Action, async: Seq[Action]) = {
		sync.run
		execute(async)
	}
}


/**
 * Container for global environment variables such as the DP services, session store, etc.
 * Often it is passed between execution nodes and entities implicitly.
 */
class Environment {
	var placement: ClusterPlacement = null
	var executor: Executor = null
	var session: Session = null
}
