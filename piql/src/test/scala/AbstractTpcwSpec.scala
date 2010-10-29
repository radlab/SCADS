package edu.berkeley.cs.scads.test

import scala.collection.JavaConversions._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.{Matcher, MatchResult, ShouldMatchers}

import org.apache.avro.generic.{GenericData, IndexedRecord}

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

import scala.collection.mutable.{ ArrayBuffer, HashMap }

abstract class AbstractTpcwSpec extends Spec with ShouldMatchers with QueryResultMatchers {
    
  val client: TpcwClient
  val loader = new TpcwLoader(client, 1, 0.1, 10)
  val data = loader.getData(0, false)
  data.load()

  describe("The TPC-W Client") {
    it("homeWI") {
      val cust = client.homeWI("cust%d".format(loader.numCustomers))
      val actual = data.customers.last
      cust should returnTuples(Array(actual._1, actual._2))
    }
    it("newProductWI") {

      val subjBuckets = 
        data.items.foldLeft(Map.empty[String, Seq[(ItemKey, ItemValue)]]) { case (map, elem @ (itemKey, itemValue)) => 
          val subj = itemValue.get(4).toString 
          map + ((subj -> (map.get(subj).getOrElse(Seq.empty) :+ elem))) 
        }

      subjBuckets foreach { case (subj, elems) => 
        val products = client.newProductWI(subj)
        products.size should equal(elems.take(50).size) // hack for now
      }

    }

  }

}
