package scaletest

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.{Environment, TrivialSession, TrivialExecutor, ZooKeptCluster}

import deploylib._
import deploylib.config._
import deploylib.runit._

import java.io.File

case class Partition(node: RunitManager, start: String, end: String)

object IntTestDeployment extends ConfigurationActions {
	def deployIntKeyLoader(target: RunitManager, start: String, end: String, server: String): RunitService = {
		createJavaService(target, new File("target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar"),
  		"scaletest.LoadIntKeys",
  		"" + start + " " + end + " " + server)
	}

	def createPartitions(numKeys: Int, nodes: List[RunitManager]):List[Partition] = {
		val partitions = new scala.collection.mutable.ArrayStack[Partition]()

		val last = (1 to numKeys by (numKeys/nodes.size)).toList.zip(nodes).reduceLeft((s,e) => {
			partitions.push(new Partition(s._2, "%010d".format(s._1), "%010d".format(e._1)))
			e
		})
		partitions.push(new Partition(last._2, "%010d".format(last._1), "%010d".format(numKeys + 1)))

		return partitions.toList.reverse
	}
}

object LoadIntKeys {
  val logger = Logger.getLogger("scads.intKeys.load")

  def main(args: Array[String]): Unit = {
		implicit val env = new Environment
  	env.placement = new ZooKeptCluster(args(2))
  	env.session = new TrivialSession
  	env.executor = new TrivialExecutor


    val startKey = args(0).toInt
    val endKey = args(1).toInt

    logger.info("Loading keys: " + startKey + " to " + endKey)

    (startKey to endKey).foreach(k => {
      val key = "%010d".format(k)
      val rec = new Record(key, "value" + k)

      env.placement.locate("intKeys", key).foreach(_.useConnection(_.put("intKeys", rec)))
    })
  }
}
