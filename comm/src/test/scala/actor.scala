package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.util.Utf8

import org.apache.log4j.Logger

object ActorSpec extends SpecificationWithJUnit("Actor Specification") {

  val lgr = Logger.getLogger("ActorSpec")

  def mkGetRequest(str: String):GetRequest = {
    val m = new GetRequest
    m.namespace = "testNS"
    m.key = str.getBytes
    m
  }

  "an actor proxy" should {
    "send/recv messages such that" >> {
      "echo works" in {
        val server = new StorageEchoServer
        server.startListener(7000)
        val resp = Sync.makeRequest(RemoteNode("localhost",7000),new Utf8("ActorTest"),mkGetRequest("test message"))
        resp must beNull
      }

      "simultaneous echos work" in {
        val server = new StorageEchoServer
        server.startListener(7001)
        var numFinished = 0
        var lock = new Object
        (1 to 10).foreach( i => {
          val t = new Thread {
            override def run = {
              val resp = Sync.makeRequest(RemoteNode("localhost",7000),new Utf8("ActorTest"),mkGetRequest("test message_thread_"+i))
              resp must beNull
              lock.synchronized {
                numFinished += 1
                if (numFinished == 10) lock.notify
              }
            }
          }
          t.start
        })
        lock.synchronized { lock.wait }
      }
    }
  }
}

class ActorTest extends JUnit4(ActorSpec)
