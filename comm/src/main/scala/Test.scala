package edu.berkeley.cs.scads

import org.apache.avro.util.Utf8
import edu.berkeley.cs.scads.comm.PrintAvroChannelManager
import edu.berkeley.cs.scads.comm.EchoAvroChannelManager
import edu.berkeley.cs.scads.comm.RemoteNode

object Receiver {
    val mgr = new PrintAvroChannelManager
    def receive(port: Int):Unit = {
        mgr.startListener(port)
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
            val record = new Record
            record.key = new Utf8("My Key " + i)
            record.value = new Utf8("My Value " + i)
            mgr.sendMessage(RemoteNode("localhost", port), record)
        })
    }
}


