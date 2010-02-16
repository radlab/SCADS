package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import org.apache.log4j.Logger

import edu.berkeley.cs.scads.comm.test.{Record,RecordSet,StorageRequest,PutRequest,GetRequest}
import edu.berkeley.cs.scads.comm.test.Storage.AvroConversions._

import edu.berkeley.cs.scads.test.helpers.Helpers
import edu.berkeley.cs.scads.test.helpers.Conversions._

import java.nio.ByteBuffer
import org.apache.avro.util.Utf8

object StorageProtocolSpec extends SpecificationWithJUnit("StorageProtocol Specification") {

    "a storage protcol" should {
        "have messages such that" >> {
            "records can be serialized/deserialized" in {
                println("test1")
                val rec = Record("key1","value1")
                val bytes = Helpers.msgToBytes(rec)
                val rec2 = Helpers.bytesToMsg(bytes, new Record)
                expectTrue(rec.equals(rec2), "Records do not match")
            }
            "lists can be assigned" in {
                println("test2")
                val list = (1 to 10).map(i => Record("key_"+i, "value_"+i)).toList
                val listcpy = (1 to 10).map(i => Record("key_"+i, "value_"+i)).toList
                val rset = RecordSet(list)
                expectTrue(rset.records.equals(listcpy), "List is not assigned properly")
            }
            "lists can be serialized/deserialized" in {
                println("test3")
                val list = (1 to 10).map(i => Record("key_"+i, "value_"+i)).toList
                val rset = RecordSet(list)
                val bytes = Helpers.msgToBytes(rset)
                expectTrue(bytes != null, "record set did not serialize properly")
                val rset2 = Helpers.bytesToMsg(bytes, new RecordSet)
                expectTrue(rset.equals(rset2), "RecordSets do not match")
                println("rset2: " + rset2)
            }
            "storage requests have a body that can be assigned" in {
                println("test4")
                val sreq = StorageRequest(12, GetRequest("myNS","myKey"))
                expectTrue(sreq.body.equals(GetRequest("myNS","myKey")), "body did not save")
                sreq.body = PutRequest("myNS", "myKey", ByteBuffer.wrap("myValue".getBytes))
                expectTrue(sreq.body.equals(PutRequest("myNS", "myKey", ByteBuffer.wrap("myValue".getBytes))), "body did not save")
            }
            "storage requests serializes/deserializes properly" in {
                println("test5")
                val sreq = StorageRequest(12, GetRequest("myNS",ByteBuffer.wrap("myKey".getBytes)))
                val bytes = Helpers.msgToBytes(sreq)
                expectTrue(bytes != null, "storageRequest set did not serialize properly")
                val sreq2 = Helpers.bytesToMsg(bytes, new StorageRequest)
                expectTrue(sreq.equals(sreq2), "StorageRequests do not match")
                println("sreq2: " + sreq2)
            }
        }
    } 

    def expectTrue(b: Boolean, m: String):Unit = {
        if (!b) fail(m)
    }

}

class StorageProtocolTest extends JUnit4(StorageProtocolSpec)
