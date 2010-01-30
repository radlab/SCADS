package edu.berkeley.cs.scads.comm

import java.nio.channels.SocketChannel
import java.net.InetSocketAddress
import java.util.concurrent.{Executor,Executors}

import org.apache.log4j.BasicConfigurator

import org.apache.avro.util._
import edu.berkeley.cs.scads._

class SimpleNioAvroServerImpl(addr: InetSocketAddress, exec: Executor) extends NioAvroServer[Record,Record](addr,exec) {

    private var counter = 0 

    override def receiveMessage(src: RemoteNode, message: Record):Unit = {
        //println("source: " + src)
        //synchronized {
        //    println("message: " + message.get(0) + " -> " + message.get(1))
        //    counter += 1
        //    println("message #:" + counter)
        //}
    }
}

object SimpleNioAvroServer {

    BasicConfigurator.configure

    def main(args: Array[String]):Unit = {
        val port = args(0).toInt
        val server = new SimpleNioAvroServerImpl(new InetSocketAddress("localhost",port), Executors.newFixedThreadPool(1)) 
        server.serve
    }

}

class SimpleNioAvroClientImpl(exec: Executor) extends NioAvroClient[Record,Record](exec) {
    override def receiveMessage(src: RemoteNode, message: Record):Unit = {
        println("source: " + src)
        println("message: " + message)
    }
}

object SimpleNioAvroClient {

    BasicConfigurator.configure

    def main(args: Array[String]):Unit = {
        val port = args(0).toInt
        val testSize = args(1).toInt
        val client = new SimpleNioAvroClientImpl(Executors.newFixedThreadPool(1)) 
        client.connect(new InetSocketAddress("localhost",port)).await
		val dest = RemoteNode("localhost", port)
		(1 to 15).foreach(t => {
			val start = System.currentTimeMillis()
            client.startBulk(dest)
			(1 to testSize).foreach(i => {
				val r = new Record
				r.key = new Utf8("testKey")
				r.value = new Utf8("testValue")
				client.sendMessage(dest, r)
			})
            client.endBulk(dest)
			val end = System.currentTimeMillis()
			println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
		})
    }

}
