package edu.berkeley.cs.scads.twitavro

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import java.net._
import java.io._
import java.util.zip.GZIPInputStream
import piql._

object ThoughtLoader {
  def main(args: Array[String]) {
    implicit val env = Configurator.configure(new ScadsCluster(new ZooKeeperProxy(args(0)).root("scads")))
    args.foreach(u => {
      if (!u.equals(args(0))) {
        try {
          println("loading: "+u)
          val url = new URL(u)
          val conn = url.openConnection
          val gin = new GZIPInputStream(conn.getInputStream)
		      val bis = new BufferedInputStream(gin)
		      val bos = new BufferedOutputStream(new FileOutputStream("/tmp/toParse"));
		      
		      var i = bis.read()
		      while (i != -1) {
			      bos.write(i)
            i = bis.read()
          }
		        bos.close()
		      bis.close()
          val it = new ThoughtJSONFileIter(new java.io.File("/tmp/toParse"))
          while(it.hasNext) {
            val x = it.next
            x._1.save
            x._2.save
          }
        } catch {
          case e => {
            println("Couldn't get: "+u)
          }
        }
      }
    })
  }
}
