package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm.Record
import edu.berkeley.cs.scads.comm.Storage.AvroConversions._

import edu.berkeley.cs.scads.test.helpers.Helpers
import edu.berkeley.cs.scads.test.helpers.Conversions._

object StorageProtocolSpec extends SpecificationWithJUnit("StorageProtocol Specification") {

    "a storage protcol" should {
        "have a record such that" >> {
            "records can be serialized/deserialized" in {
                val rec = Record("key1","value1")
                val bytes = Helpers.msgToBytes(rec)
                val rec2 = Helpers.bytesToMsg(bytes, new Record)
                println(rec)
                println(rec2)
            }
        }
    } 

}

class StorageProtocolTest extends JUnit4(StorageProtocolSpec)
