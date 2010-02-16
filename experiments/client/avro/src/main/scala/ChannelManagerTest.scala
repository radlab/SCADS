package edu.berkeley.cs.scads.avro.test

import edu.berkeley.cs.scads.comm.{RemoteNode,NioAvroChannelManagerBase}
import edu.berkeley.cs.scads.comm.test.{Record,RecordSet,StorageRequest,PutRequest,GetRequest,StorageResponse,GetRangeRequest,KeyRange}
import edu.berkeley.cs.scads.comm.test.Storage.AvroConversions._

import edu.berkeley.cs.scads.test.helpers.Conversions._
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class PrintAvroChannelManager extends NioAvroChannelManagerBase[StorageRequest, StorageResponse] {
    override def receiveMessage(src: RemoteNode, msg: StorageResponse):Unit = {
        println("src: " + src)
        println("msg: " + msg)
    }
}

class EchoAvroChannelManager extends NioAvroChannelManagerBase[StorageResponse, StorageRequest] {
    override def receiveMessage(src: RemoteNode, msg: StorageRequest):Unit = {
        println("src: " + src)
        println("msg: " + msg)
        msg.body match {
            case GetRequest(namespace,key) => 
                sendMessage(src, StorageResponse(msg.src, Record(key, ByteBuffer.wrap("value".getBytes))))
            case GetRangeRequest(namespace, keyRange) => 
                sendMessage(src, StorageResponse(msg.src, 
                            RecordSet((1 to 3).map( i => Record("key"+i, "value"+i) ).toList)))
            case _ => println("Unknown message type")
        }
    }
}


object EchoReceiver {
    val mgr = new EchoAvroChannelManager
    def receive(port: Int):Unit = {
        mgr.startListener(port)
    }
}

object Sender {
    val mgr = new PrintAvroChannelManager
    def send(port: Int):Unit = {
        (1 to 10).foreach( i => {
            mgr.sendMessage(RemoteNode("localhost", port), StorageRequest(555, GetRequest("namespace", "key"+i)))
        })
        (1 to 5).foreach( i => {
            mgr.sendMessage(
                RemoteNode("localhost", port), 
                StorageRequest( 555, GetRangeRequest(
                        "namespace", 
                        KeyRange(null, null, 10, 100, false))))
        })
    }
}
