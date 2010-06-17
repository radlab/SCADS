package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._

import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger

import com.googlecode.avro.marker.AvroRecord
import com.googlecode.avro.annotation.AvroUnion

case class TestMsg(var payload: String) extends AvroRecord

object ChannelManagerSpec extends SpecificationWithJUnit("ChannelManager Specification") {

  class TestChannelManager extends NioAvroChannelManagerBase[TestMsg, TestMsg] {

    val logger = Logger.getLogger("TestChannelManager")

      /* msg -> time received */
    private val received = new ConcurrentHashMap[String,Long]

    override def receiveMessage(src: RemoteNode, msg: TestMsg): Unit = {
      logger.debug("message " + msg + " received from " + src)
      received.put(msg.payload, System.currentTimeMillis) 
    }

    def hasReceived(msgs: String, time: Int):Boolean = hasReceived(List(msgs), time)

      def hasReceived(msgs: List[String], time: Int): Boolean = {
      logger.debug("hasReceived() called")
      val endTime = System.currentTimeMillis + time.toLong*1000
      var done = false
      while (!done && System.currentTimeMillis < endTime) {
        done = msgs.filter(received.containsKey(_)).size == msgs.size
        if (done) return true
        Thread.sleep(500)
      }
      done
    }

    def reset = received.clear

  }

  class EchoTestChannelManager extends TestChannelManager {
    override def receiveMessage(src: RemoteNode, msg: TestMsg): Unit = {
      super.receiveMessage(src, msg)
      sendMessage(src, msg)
    }
  }
  val lgr = Logger.getLogger("ChannelManagerSpec")

    implicit def string2testmsg(str: String):TestMsg = {
    val m = classOf[TestMsg].newInstance
    m.payload = str
    m
  }

  implicit def string2utf8(str: String):String = { new String(str) }

  def mustBeTrue(b: Boolean, m: String) = {
    if(!b) fail(m)
    }



  "a channel manager" should {
    "send/recv messages such that" >> {
      "listeners receive one message" in {
        lgr.debug("begin test 1")
        val client = new TestChannelManager
        val server = new TestChannelManager
        server.startListener(8080)
        client.sendMessage(RemoteNode("localhost",8080), "test1") 
          mustBeTrue(server.hasReceived("test1",10),"must have received message test1")
            server.reset
            lgr.debug("end test 1")
          }

          "listeners receive >1 message" in {
            lgr.debug("begin test 2")
            val client = new TestChannelManager
            val server = new TestChannelManager
            server.startListener(8081)
            val msgs = List("a","b","c","123","456")
              msgs.foreach(client.sendMessage(RemoteNode("localhost",8081),_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 5 msgs")
            server.reset
            lgr.debug("end test 2")
          }

          "listeners receive simultaneous msgs" in {
            lgr.debug("begin test 3")
            val client = new TestChannelManager
            val server = new TestChannelManager
            server.startListener(8082)
            var allMsgs = List[String]()
              (1 to 5).foreach(i => {
                val msgs = (1 to 100).map(j => "thread"+i+"_"+j+"_msg").toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(m => client.sendMessage(RemoteNode("localhost",8082),m))
                  }
                }
                t.start
              })
            mustBeTrue(server.hasReceived(allMsgs.map(string2utf8(_)),10),"must have recv-ed all 5 msgs from diff threads")
            server.reset
            lgr.debug("end test 3")
          }

          "echo protocol works" in {
            lgr.debug("begin test 4")
            val client = new TestChannelManager
            val server = new EchoTestChannelManager
            server.startListener(8083)
            val msgs = List("a","b","c","123","456")
              msgs.foreach(client.sendMessage(RemoteNode("localhost",8083),_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"server must have recv-ed all msgs")
            mustBeTrue(client.hasReceived(msgs.map(string2utf8(_)),10),"client must have recv-ed all echos") 
            lgr.debug("end test 4")
          }

          "multiple client echo protocol works" in {
            lgr.debug("begin test 5")
            val client = new TestChannelManager
            val server = new EchoTestChannelManager
            server.startListener(8084)
            var allMsgs = List[String]()
              (1 to 5).foreach(i => {
                val msgs = List("a","b","c","123","456").map("thread"+i+"_"+_).toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(client.sendMessage(RemoteNode("localhost",8084),_))
                  }
                }
                t.start
              })
            mustBeTrue(server.hasReceived(allMsgs.map(string2utf8(_)),10),"server must have recv-ed all msgs from diff threads") 
            mustBeTrue(client.hasReceived(allMsgs.map(string2utf8(_)),10),"client must have recv-ed all echos")
            server.reset
            lgr.debug("end test 5")
          }

          "sending a lot of messages works" in {
            lgr.debug("begin test 6")
            val client = new TestChannelManager
            val server = new TestChannelManager
            server.startListener(8085)
            val msgs = (1 to 100).map(i => "msg_"+i).toList
            msgs.foreach(client.sendMessage(RemoteNode("localhost",8085),_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 100 msgs")
            server.reset
            lgr.debug("end test 6")
          }

          "bulk sending works" in {
            lgr.debug("begin test 7")
            val client = new TestChannelManager
            val server = new TestChannelManager
            server.startListener(8086)
            val msgs = (1 to 100).map(i => "msg_"+i).toList
            msgs.foreach(client.sendMessageBulk(RemoteNode("localhost",8086),_))
            client.flush
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 100 msgs")
            server.reset
            lgr.debug("end test 7")
          }

          "bulk sending from multiple threads works" in {
            lgr.debug("begin test 8")
            val client = new TestChannelManager
            val server = new TestChannelManager
            server.startListener(8087)
            var allMsgs = List[String]()
              var count = 0
            val lock = new Object
            (1 to 5).foreach(i => {
                val msgs = (1 to 100).map(j => "thread"+i+"_"+j+"_msg").toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(m => client.sendMessageBulk(RemoteNode("localhost",8087),m))
                    lock.synchronized { 
                      count += 1
                      if (count == 5) lock.notify
                    }
                  }
                }
                t.start
              })
            lock.synchronized { while (count < 5) lock.wait }
            client.flush
            mustBeTrue(server.hasReceived(allMsgs.map(string2utf8(_)),10),"must have recv-ed all 100 msgs from each diff threads")
            server.reset
            lgr.debug("end test 8")
          }
        }
      }
    }

    class ChannelManagerTest extends JUnit4(ChannelManagerSpec)
