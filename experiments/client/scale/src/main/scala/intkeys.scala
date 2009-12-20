package scaletest

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}

import deploylib._
import deploylib.config._
import deploylib.runit._
import deploylib.xresults._
import scala.xml._
import scala.util.Random

import java.io.File
import scala.concurrent.SyncVar


case class Partition(start: String, end: String)

object IntTestDeployment extends ConfigurationActions {
	def createPartitions(numKeys: Int, numPartitions: Int):List[Partition] = createPartitions(1, numKeys, numPartitions)
	def createPartitions(startKey: Int, endKey: Int, numPartitions: Int):List[Partition] = {
		val partitions = new scala.collection.mutable.ArrayStack[Partition]()
		val numKeys = endKey - startKey + 1

		val last = (startKey to endKey by (numKeys/numPartitions)).toList.reduceLeft((s,e) => {
			partitions.push(new Partition("%010d".format(s), "%010d".format(e)))
			e
		})
		partitions.push(new Partition("%010d".format(last), "%010d".format(numKeys + startKey)))

		return partitions.toList.reverse
	}
}

class NoNodeResponsibleException extends RetryableException

abstract class IntKeyTest extends ScadsTest {
	def makeKey(key: Int) = "%010d".format(key)
	def makeRecord(key: Int) = new Record(makeKey(key), "value" + key)
	def locateKey(key: Int):StorageNode = locateKey(makeKey(key))
	def locateKey(key: String): StorageNode = {
		val nodes = env.placement.locate("intKeys", key)
		if(nodes.size == 0) {
			logger.fatal("No node responsible for key: " + key)
			throw new NoNodeResponsibleException
		}
		return nodes(0)
	}

	def getMaxKey(): Int = {
		env.placement.asInstanceOf[ZooKeptCluster].getPolicies("intKeys").map(p => {
			p._2.policy.apply(0)._2.toInt
		}).foldLeft(0)((a: Int, b: Int) => {
			if(a > b) (a - 1) else (b - 1)
		})
	}

	def putKey(key: Int): Unit = {
		val nodes = env.placement.locate("intKeys", makeKey(key))
		if(nodes.size == 0) {
			logger.fatal("No node responsible for key: " + key)
			throw new NoNodeResponsibleException
		}
		nodes.foreach(_.useConnection(_.put("intKeys", makeRecord(key))))
	}

	def getKey(key: Int): Boolean = {
		val skey = makeKey(key)
		val result = locateKey(skey).useConnection(_.get("intKeys", skey))

		if(result.value == null) {
			logger.warn("Key missing: " + skey)
			false
		}
		else if(!result.value.equals("value" + key)) {
			logger.warn("For key: " + skey + " got: " + result.value)
			false
		}
		else
			true
	}
}

object ThreadedLoader extends IntKeyTest {
	options.addOption("s", "startKey", true, "first key to load")
	options.addOption("e", "endKey", true, "last key (exclusive)")
	options.addOption("t", "threads", true, "number of threads to split the load into")

	def runExperiment(): NodeSeq = {
		val startKey = getIntOption("startKey")
		val endKey = getIntOption("endKey")

		logger.info("Loading running from " + startKey + " to " + endKey)

		val results = IntTestDeployment.createPartitions(startKey, endKey - 1, getIntOption("threads")).toList.map(p => {
			logger.info("Creating thread for range: " + p)
			Future {
				XResult.benchmark {
					Util.retry(10) {
						XResult.recordException {
							runTest(p.start.toInt, p.end.toInt)
							<sequentialLoad>
								<startKey>{p.start.toInt}</startKey>
								<endKey>{p.end.toInt}</endKey>
								<method>{this.getClass.getName}</method>
							</sequentialLoad>
						}
					}
				}
			}
		})

		results.map(r => <thread>{r()}</thread>)
	}

	def runTest(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 10000 == 0)
				logger.info("Adding key " + k)
			putKey(k)
    })
		logger.info("Thread Complete")
  }
}

object RandomReader extends IntKeyTest with ThreadedScadsExperiment {
	lazy val maxKey = getMaxKey()
	options.addOption("l", "length", true, "the length in seconds for the test to run")

	def runThread: NodeSeq = {
		val seed = (XResult.hostname + Thread.currentThread().getName + System.currentTimeMillis).hashCode
		val rand = new Random(seed)

		XResult.timeLimitBenchmark(getIntOption("length"), 1, <randomReads><maxKey>{maxKey.toString}</maxKey></randomReads>) {
			try {
				getKey(rand.nextInt(maxKey) + 1)
			}
			catch {
				case e: NoNodeResponsibleException => false
			}
		}
	}
}
