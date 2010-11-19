package edu.berkeley.cs.scads.piql

import _root_.edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._
import org.apache.log4j.Logger
import collection.mutable.{HashSet, ArrayBuffer}

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

    //client.loadData(0.1, 100)
    //println("cust1 interaction " + client.homeWI("cust1").flatMap(_.map(_.toString)))
    //val subject = client.item.getRange(None,None).head._2.I_SUBJECT
    //println("newProduct: " + client.newProductWI(subject).flatMap(_.map(_.toString)))
    //val authorName = client.author.getRange(None,None).head._2.A_LNAME;
    //println("NameIndex " + client.authorNameItemIndex.getRange(None,None))
    //println("authorSearch: " +  client.searchByAuthorWI(authorName).flatMap(_.map(_.toString)))

    //println("searchBySubjectPlan: " + client.searchBySubjectWI(subject).flatMap(_.map(_.toString)))

    //val titleToken = client.item.getRange(None,None).head._2.I_TITLE.split("\\s+").head
    //println("searchByTitlePlan " +  client.searchByTitleWI(titleToken))

    //var cust = new ArrayBuffer[String]()
    //cust ++= client.customer.getRange(None, None).map(_._1.C_UNAME)
    //val subjSet = new HashSet[String]
    //subjSet ++= client.item.getRange(None, None).map(_._2.I_SUBJECT)
    //val subjects = new ArrayBuffer[String]()
    //subjects ++= subjSet
    //val authorsSet =  new HashSet[String]
    //authorsSet ++= client.author.getRange(None, None).flatMap(a => List(a._2.A_FNAME, a._2.A_LNAME))
    //val authors = new ArrayBuffer[String]()
    //authors ++= authorsSet


    //val  workflow = new TpcwWorkflow(client, cust, authors, subjects)
    //(1 to 100).foreach(_ => workflow.executeMix() )
  }
}

object TpcwTest {

  def main(args: Array[String]) {
    var test = new TpcwTest
    test.run()
  }

}
