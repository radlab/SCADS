package edu.berkeley.cs.scads.comm

import java.nio._
import java.nio.channels._
import java.net.InetSocketAddress

import org.apache.avro.io._
import org.apache.avro.ipc._
import org.apache.avro.specific._

import scala.collection.jcl.Conversions._

case class RemoteNode(hostname:String, port: Int) {
    private val socketAddress = new InetSocketAddress(hostname, port) 
    def getInetSocketAddress:InetSocketAddress = socketAddress 
}

trait AvroChannelManager[SendMsgType <: SpecificRecord, RecvMsgType <: SpecificRecord] {
    def startBulk(chan: RemoteNode)
    def endBulk(chan: RemoteNode)
	def sendMessage(dest: RemoteNode, msg: SendMsgType):Unit 
	def receiveMessage(src: RemoteNode, msg: RecvMsgType):Unit
}
