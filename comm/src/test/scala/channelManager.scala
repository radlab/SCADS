package edu.berkeley.cs.scads.comm.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import edu.berkeley.cs.scads.comm._

import java.util.concurrent.ConcurrentHashMap

import net.lag.logging.Logger

import com.googlecode.avro.marker.AvroRecord

case class TestMsg(var payload: String) extends AvroRecord

@RunWith(classOf[JUnitRunner])
class ChannelManagerSpec extends Spec with ShouldMatchers {
  var currentPort = 9000

  class TestChannelManager extends NioAvroChannelManagerBase[TestMsg, TestMsg] {

      /* msg -> time received */
    private val received = new ConcurrentHashMap[String,Long]

    startListener()

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
  val lgr = Logger()

    implicit def string2testmsg(str: String):TestMsg = {
    val m = classOf[TestMsg].newInstance
    m.payload = str
    m
  }

  implicit def string2utf8(str: String):String = { new String(str) }

  def mustBeTrue(b: Boolean, m: String) = {
    if(!b) fail(m)
    }



  describe("a channel manager") {
    describe("send/recv messages such that") {
      it("listeners receive one message") {
        lgr.debug("begin test 1")
        val client = new TestChannelManager
        val server = new TestChannelManager
        client.sendMessage(server.remoteNode, "test1")
          mustBeTrue(server.hasReceived("test1",10),"must have received message test1")
            server.reset
            lgr.debug("end test 1")
          }

          it("listeners receive >1 message") {
            lgr.debug("begin test 2")
            val client = new TestChannelManager
            val server = new TestChannelManager
            val msgs = List("a","b","c","123","456")
              msgs.foreach(client.sendMessage(server.remoteNode,_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 5 msgs")
            server.reset
            lgr.debug("end test 2")
          }

          it("listeners receive simultaneous msgs") {
            lgr.debug("begin test 3")
            val client = new TestChannelManager
            val server = new TestChannelManager
            var allMsgs = List[String]()
              (1 to 5).foreach(i => {
                val msgs = (1 to 100).map(j => "thread"+i+"_"+j+"_msg").toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(m => client.sendMessage(server.remoteNode,m))
                  }
                }
                t.start
              })
            mustBeTrue(server.hasReceived(allMsgs.map(string2utf8(_)),10),"must have recv-ed all 5 msgs from diff threads")
            server.reset
            lgr.debug("end test 3")
          }

          it("echo protocol works") {
            lgr.debug("begin test 4")
            val client = new TestChannelManager
            val server = new EchoTestChannelManager
            val msgs = List("a","b","c","123","456")
              msgs.foreach(client.sendMessage(server.remoteNode,_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"server must have recv-ed all msgs")
            mustBeTrue(client.hasReceived(msgs.map(string2utf8(_)),10),"client must have recv-ed all echos")
            lgr.debug("end test 4")
          }

          it("multiple client echo protocol works") {
            lgr.debug("begin test 5")
            val client = new TestChannelManager
            val server = new EchoTestChannelManager
            var allMsgs = List[String]()
              (1 to 5).foreach(i => {
                val msgs = List("a","b","c","123","456").map("thread"+i+"_"+_).toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(client.sendMessage(server.remoteNode,_))
                  }
                }
                t.start
              })
            mustBeTrue(server.hasReceived(allMsgs.map(string2utf8(_)),10),"server must have recv-ed all msgs from diff threads")
            mustBeTrue(client.hasReceived(allMsgs.map(string2utf8(_)),10),"client must have recv-ed all echos")
            server.reset
            lgr.debug("end test 5")
          }

          it("sending a lot of messages works") {
            lgr.debug("begin test 6")
            val client = new TestChannelManager
            val server = new TestChannelManager
            val msgs = (1 to 100).map(i => "msg_"+i).toList
            msgs.foreach(client.sendMessage(server.remoteNode,_))
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 100 msgs")
            server.reset
            lgr.debug("end test 6")
          }

          it("bulk sending works") {
            lgr.debug("begin test 7")
            val client = new TestChannelManager
            val server = new TestChannelManager
            val msgs = (1 to 100).map(i => "msg_"+i).toList
            msgs.foreach(client.sendMessageBulk(server.remoteNode,_))
            client.flush
            mustBeTrue(server.hasReceived(msgs.map(string2utf8(_)),10),"must have recv-ed all 100 msgs")
            server.reset
            lgr.debug("end test 7")
          }

          it("bulk sending from multiple threads works") {
            lgr.debug("begin test 8")
            val client = new TestChannelManager
            val server = new TestChannelManager
            var allMsgs = List[String]()
              var count = 0
            val lock = new Object
            (1 to 5).foreach(i => {
                val msgs = (1 to 100).map(j => "thread"+i+"_"+j+"_msg").toList
                allMsgs = allMsgs ::: msgs
                val t = new Thread {
                  override def run = {
                    msgs.foreach(m => client.sendMessageBulk(server.remoteNode,m))
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
