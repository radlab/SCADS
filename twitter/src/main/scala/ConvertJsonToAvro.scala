package edu.berkeley.cs
package twitterspam

import scalaj.collection.Imports._

import avro.marker._
import avro.runtime._
import scads.storage._
import scads.perf._
import scads.comm._
import deploylib.mesos._

import org.apache.avro.file.CodecFactory

case class LabeledTweet(var path: String, var label: Int) extends AvroRecord
case class TwitterSpamRecord(var hash: String, var year: Int, var month: Int, var day: Int, var hour: Int, var logType: String) extends AvroPair {
  var label: Int = _
  var features: collection.Map[String, Double] = _
}

object TestLocal {
  val fileUrl = "http://cs.berkeley.edu/~marmbrus/tmp/labeledTweets.avro"
  def main(args: Array[String]): Unit = LoadJsonToScadsTask(fileUrl, TestScalaEngine.newScadsCluster().root.canonicalAddress).run

}

case class LoadJsonToScadsTask(var fileListUrl: String, var clusterAddress: String) extends AvroTask with AvroRecord {
  def run() = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val fileList = AvroHttpFile[LabeledTweet](fileListUrl)
    val PathRegEx = """(\d+)-(\d+)-(\d+)\/(\d+)\/([^\.]+)\.([^\.]+)\.log""".r
    val spamRecords = fileList.map(f => {
      var json = GetJson.getJson(f.path) // Fetches the text of the file sitting in 'path'
      val vec = ParseData.parseData(json, Array("skip_kestrel", "skip_email", "skip_individual_ips", "skip_tweet"))

      val PathRegEx(year, month, day, hour, hash, logType) = f.path
      val rec = new TwitterSpamRecord(hash, year.toInt, month.toInt, day.toInt, hour.toInt, logType)
      rec.label = f.label
      val features = vec.elements.asScala
      rec.features = features
      println("Record has " + features.size + " features")
      rec
    })
    val servers = cluster.getAvailableServers.map(_ :: Nil)
    val splitPoints = ((1 to 9).map(_.toString) ++ List("a", "b", "c", "d", "e", "f"))
    val splits = None +: splitPoints.map(p => {
      new TwitterSpamRecord(p, 0,0,0,0,"").key
    }).map(Option.apply)
    val ns = cluster.createNamespace[TwitterSpamRecord]("twitterSpamRecords", splits.zip(servers))

    ns ++= spamRecords
    clusterRoot.createChild("DataLoaded")
  }
}


/**
 * Helper function to encode the file_list_fold_X
 */
object EncodeTweetLabels {
  def main(args: Array[String]): Unit = {
    val avroFile = AvroOutFile[LabeledTweet]("labeledTweets.avro", CodecFactory.deflateCodec(9))

    for(i <- (0 to 9)) {
      val lines = scala.io.Source.fromFile("file_list_fold_%d".format(i)).getLines
      val records = lines.map(_.split(" ")).foreach {
	case Array(label, path) => avroFile.append(LabeledTweet(path, label.toInt))
      }
    }
    avroFile.close
  }
}
