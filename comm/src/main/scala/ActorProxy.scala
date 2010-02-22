package edu.berkeley.cs.scads.comm

import java.lang.ref.WeakReference
import java.lang.ref.ReferenceQueue
import java.util.concurrent.ConcurrentHashMap

import scala.actors._

import org.apache.log4j.Logger

class StorageEchoServer extends NioAvroChannelManagerBase[Message, Message] {
	def receiveMessage(src: RemoteNode, req: Message): Unit = {
		val resp = new Message
		resp.dest = req.src
		sendMessage(src, resp)
	}
}

class StorageEchoPrintServer extends StorageEchoServer {
    val lock = new Object
    var numMsgs = 0
    override def receiveMessage(src: RemoteNode, req: Message): Unit = {
      lock.synchronized {
        numMsgs += 1
        if (numMsgs % 100000 == 0) println("On msg: " + numMsgs)
      }
      super.receiveMessage(src, req)
	  }
}

class StorageDiscardServer extends NioAvroChannelManagerBase[Message, Message] {
	def receiveMessage(src: RemoteNode, req: Message): Unit = { }
}

@deprecated
class StorageActorProxy extends NioAvroChannelManagerBase[Message, Message] {
	def receiveMessage(src: RemoteNode, msg: Message): Unit = {//msg.dest match {
    //case l:java.lang.Long => {
      //ActorRegistry.sendMessage(l.longValue, (src, msg))
		MessageHandler.receiveMessage(src,msg)
    //}
    //case _ => {
    //  throw new Exception("Trying to receive a message for an actor that didn't specify an integer id");
  }
}
