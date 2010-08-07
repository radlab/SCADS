package edu.berkeley.cs.scads.test
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import edu.berkeley.cs.scads.storage._

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class RoutingTableSpec extends WordSpec with ShouldMatchers {

  "A Routing Table" should {

    "create routing table" is (pending)

    "return partitions for key" is (pending)

    "return partitions for a range" is (pending)

    "be able to split partitions" is (pending)

    "inrease the replication level of an partition" is (pending)

    "delete an replica of an partition" is (pending)

    "gets notified about routing table changes" is (pending)

    "when merge partitions" should {
      "merge partitions" is (pending)

      "throw an error if configuration differ" is (pending)

    }

  }
}
