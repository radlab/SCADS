package edu.berkeley.cs.scads.piql

import _root_.edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger



/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Oct 13, 2010
 * Time: 5:51:47 PM
 * To change this template use File | Settings | File Templates.
 */

class TpcwTest {
  implicit def toOption[A](a: A): Option[A] = Option(a)
   val storageHandler = TestScalaEngine.getTestHandler(3)
   val cluster = new ScadsCluster(storageHandler.head.root)
   var client = new TpcwClient(cluster, new SimpleExecutor)
  
  def run() = {

    client.loadData(0.1, 100)
    println("cust1 interaction " + client.homeWI("cust1").flatMap(_.map(_.toString)))
  }
}

object TpcwTest {

  def main(args: Array[String]) {
    var test = new TpcwTest
    test.run()
  }

}