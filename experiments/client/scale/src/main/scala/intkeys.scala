package scaletest

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}

import deploylib._
import deploylib.config._
import deploylib.runit._
import deploylib.xresults._
import scala.xml._

import java.io.File
import scala.concurrent.SyncVar


case class Partition(start: String, end: String)

object IntTestDeployment extends ConfigurationActions {
	def deployLoadClient(target: RunitManager, cl: String, args: String):RunitService = {
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
  		"scaletest." + cl,
  		512,
			args)
	}

	def deployIntKeyLoader(target: RunitManager, name: String, start: String, end: String, server: String): RunitService = {
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
  		"scaletest." + name,
  		512,
			"" + start + " " + end + " " + server)
	}

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

abstract class IntKeyTest {
	val logger = Logger.getLogger("scads.intKeyTest")
	implicit val env = new Environment

	def configEnv(zooServer: String):Unit = {
		env.placement = new ZooKeptCluster(zooServer)
  	env.session = new TrivialSession
  	env.executor = new TrivialExecutor
	}
	def makeKey(key: Int) = "%010d".format(key)
	def makeRecord(key: Int) = new Record(makeKey(key), "value" + key)
	def locateKey(key: Int):StorageNode = locateKey(makeKey(key))
	def locateKey(key: String): StorageNode = {
		val nodes = env.placement.locate("intKeys", key)
		if(nodes.size == 0)
			throw new NoNodeResponsibleException
		return nodes(0)
	}

	def getMaxKey: Int = {
		env.placement.asInstanceOf[ZooKeptCluster].getPolicies("intKeys").map(p => {
			p._2.policy.apply(0)._2.toInt
		}).foldLeft(0)((a: Int, b: Int) => {
			if(a > b) a else b
		})
	}
}

abstract class KeyRangeTest extends IntKeyTest {

  def main(args: Array[String]): Unit = {
   	configEnv(args(2))
		val startKey = args(0).toInt
    val endKey = args(1).toInt

    logger.info("Running test " + this.getClass.getName + " on keys " + startKey + " to " + endKey)

    XResult.recordResult {
      XResult.benchmark {
        run(startKey, endKey)
        <sequentialLoad>
          <startKey>{startKey.toString}</startKey>
          <endKey>{endKey.toString}</endKey>
          <method>{this.getClass.getName}</method>
        </sequentialLoad>
      }
    }
  }

	def run(startKey: Int, endKey: Int): Unit
}


object ThreadedLoader extends IntKeyTest {
	def main(args: Array[String]): Unit = {
		configEnv(args(0))
		val startKey = args(1).toInt
		val endKey = args(2).toInt
		val numThreads = args(3).toInt

		logger.info("Loading running from " + startKey + " to " + endKey)

		val results = IntTestDeployment.createPartitions(startKey, endKey - 1, numThreads).toList.map(p => {
			logger.info("Creating thread for range: " + p)
			val result = new SyncVar[scala.xml.Elem]
			val thread = new Thread() {
				override def run() = {
					result.set(XResult.benchmark {
						runTest(p.start.toInt, p.end.toInt)
						<sequentialLoad>
							<startKey>{p.start.toInt}</startKey>
							<endKey>{p.end.toInt}</endKey>
							<method>{this.getClass.getName}</method>
						</sequentialLoad>
      		})
				}
			}
			thread.start()
			result
		})

		XResult.recordResult(results.map(r => <thread>{r.get}</thread>))
	}

	def runTest(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)

			XResult.retryAndRecord(10)(() => {
      	val nodes = env.placement.locate("intKeys", makeKey(k))
				if(nodes.size == 0)
					throw new NoNodeResponsibleException
				nodes.foreach(_.useConnection(_.put("intKeys", makeRecord(k))))
			})
    })
  }
}


object SingleConnectionPoolLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)

			XResult.retryAndRecord(10)(() => {
      	val nodes = env.placement.locate("intKeys", makeKey(k))
				if(nodes.size == 0)
					throw new NoNodeResponsibleException
				nodes.foreach(_.useConnection(_.put("intKeys", makeRecord(k))))
			})
    })
  }
}

object SingleAsyncConnectionPoolLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)

			XResult.retryAndRecord(10)(() => {
      	val nodes = env.placement.locate("intKeys", makeKey(k))
				if(nodes.size == 0)
					throw new NoNodeResponsibleException

				nodes.foreach(_.useConnection(_.async_put("intKeys", makeRecord(k))))
			})
    })
  }
}


object SingleConnectionLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    val nodes = env.placement.locate("intKeys", makeKey(startKey))
		if(nodes.size == 0)
				throw new NoNodeResponsibleException

		if(nodes.size > 1)
			logger.warn("Not designed for replicated envs")
		var conn = nodes(0).getConnection
		logger.info("Using connection: " + conn)

    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)
			XResult.retryAndRecord(10)(() => {
				conn.put("intKeys", makeRecord(k))
			})
    })
  }
}

object SingleAsyncConnectionLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    val nodes = env.placement.locate("intKeys", makeKey(startKey))
		if(nodes.size == 0)
				throw new NoNodeResponsibleException

		if(nodes.size > 1)
			logger.warn("Not designed for replicated envs")
		var conn = nodes(0).getConnection
		logger.info("Using connection: " + conn)

    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)
			XResult.retryAndRecord(10)(() => {
				conn.async_put("intKeys", makeRecord(k))
			})
    })
  }
}

object SingleRandomReader extends IntKeyTest {
	def main(args: Array[String]): Unit = {
		configEnv(args(0))
		val maxKey = getMaxKey
		val timeout = args(1).toInt
		val rand = new Random

		XResult.recordResult(
			XResult.timeLimitBenchmark(timeout, 100, <randomRead/>) {
				val key = rand.nextInt(maxKey)
				val skey = makeKey(key)
				val result = locateKey(skey).useConnection(_.get("intKeys", skey))

				if(result.value == null)
					logger.warn("Key missing: " + skey)
				else if(!result.value.equals("value" + key))
					logger.warn("For key: " + skey + " got: " + result.value)
			}
		)
	}
}
