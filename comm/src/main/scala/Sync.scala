package edu.berkeley.cs.scads.comm

import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

import org.apache.log4j.Logger
import org.apache.avro.util.Utf8

@deprecated("use methods in RemoteActor")
object Sync {
	val logger = Logger.getLogger("scads.comm.sync")

	def makeRequest(dest: RemoteActor, reqBody: MessageBody, timeout:Long = 10000): Object = dest !?(reqBody, timeout)

	def makeRequestNoTimeout(dest: RemoteActor, reqBody: MessageBody): Object = dest !?(reqBody, 100000)
}
