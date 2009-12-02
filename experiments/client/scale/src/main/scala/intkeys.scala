package scaletest

import org.apache.log4j.Logger

import deploylib.configuration.JavaService
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model.DefaultEnvironment

class IntKeyLoader(start: Int, end: Int) extends JavaService(
  "target/scale-1.0-SNAPSHOT-jar-with-dependencies.jar",
  "scaletest.LoadIntKeys",
  "" + start + " " + end)

object LoadIntKeys {
  implicit val env = DefaultEnvironment
  val logger = Logger.getLogger("scads.intKeys.load")

  def main(args: Array[String]): Unit = {
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
