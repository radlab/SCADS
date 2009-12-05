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

case class Partition(start: String, end: String)

object IntTestDeployment extends ConfigurationActions {
	def deployIntKeyLoader(target: RunitManager, name: String, start: String, end: String, server: String): RunitService = {
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
  		"scaletest." + name,
  		512,
			"" + start + " " + end + " " + server)
	}

	def createPartitions(numKeys: Int, numPartitions: Int):List[Partition] = {
		val partitions = new scala.collection.mutable.ArrayStack[Partition]()

		val last = (1 to numKeys by (numKeys/numPartitions)).toList.reduceLeft((s,e) => {
			partitions.push(new Partition("%010d".format(s), "%010d".format(e)))
			e
		})
		partitions.push(new Partition("%010d".format(last), "%010d".format(numKeys + 1)))

		return partitions.toList.reverse
	}
}

class NoNodeResponsibleException extends Exception

abstract class IntKeyTest {
	val logger = Logger.getLogger("scads.intKeyTest")

	def makeKey(key: Int) = "%010d".format(key)
	def makeRecord(key: Int) = new Record(makeKey(key), "value" + key)
}

abstract class KeyRangeTest extends IntKeyTest {
	implicit val env = new Environment

  def main(args: Array[String]): Unit = {
  	env.placement = new ZooKeptCluster(args(2))
  	env.session = new TrivialSession
  	env.executor = new TrivialExecutor

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



object SingleConnectionPoolLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)

      val nodes = env.placement.locate("intKeys", makeKey(k))
			if(nodes.size == 0)
				throw new NoNodeResponsibleException

			nodes.foreach(_.useConnection(_.put("intKeys", makeRecord(k))))
    })
  }
}

object SingleAsyncConnectionPoolLoader extends KeyRangeTest {
	def run(startKey: Int, endKey: Int): Unit = {
    (startKey to (endKey - 1)).foreach(k => {
			if(k % 1000 == 0)
				logger.info("Adding key " + k)

      val nodes = env.placement.locate("intKeys", makeKey(k))
			if(nodes.size == 0)
				throw new NoNodeResponsibleException

			nodes.foreach(_.useConnection(_.async_put("intKeys", makeRecord(k))))
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

			conn.put("intKeys", makeRecord(k))
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

			conn.async_put("intKeys", makeRecord(k))
    })
  }

}
