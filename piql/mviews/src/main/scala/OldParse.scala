import edu.berkeley.cs.avro._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._
import edu.berkeley.cs.scads.perf.Histogram
import edu.berkeley.cs.scads.piql.tpcw.scale.TpcwWorkflowTask
import edu.berkeley.cs.scads.piql.tpcw.TpcwLoaderTask

case class Result(var expId: String,
                  var clientConfig: TpcwWorkflowTask,
                  var loaderConfig: TpcwLoaderTask,
                  var clusterAddress: String,
                  var clientId: Int,
                  var iteration: Int,
                  var threadId: Int) extends AvroPair {
  //   var totalElaspedTime: Long = _ /* in ms */
  var times: Seq[Histogram] = null
  //    var getTimes: Seq[Histogram] = null
  //    var getRangeTimes: Seq[Histogram] = null
  var skips: Int = _
  var failures: Int = _
}

object ParseOldStyle {

  def main(args: Array[String]): Unit = {
    val data = AvroInFile[Result](new java.io.File(args(0)))

    (data.filter(_.iteration != 1).toSeq
      .groupBy(r => (r.loaderConfig.numServers, r.clientConfig.expId)).toSeq
      .sortBy(_._1)
      .foreach {
      case (size, results) =>
        val aggResults = results.flatMap(_.times).reduceLeft(_ + _)
        println(Seq(size._1, size._2, aggResults.quantile(0.90), aggResults.quantile(0.99), aggResults.stddev, aggResults.totalRequests).mkString("\t"))

    })
  }
}