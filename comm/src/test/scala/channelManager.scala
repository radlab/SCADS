package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import java.util.concurrent.ConcurrentHashMap

import org.apache.log4j.Logger

object ChannelManagerSpec extends SpecificationWithJUnit("ChannelManager Specification") {
    class TestChannelManager extends AvroChannelManager[TestMsg, TestMsg] {

        val logger = Logger.getLogger("TestChannelManager")

        /* msg -> time received */
        private val received = new ConcurrentHashMap[TestMsg,Long]

        def receiveMessage(src: RemoteNode, msg: TestMsg): Unit = {
            logger.debug("message " + msg + " received from " + src)
            received.put(msg, System.currentTimeMillis) 
        }

        def hasReceived(msgs: TestMsg, time: Int):Boolean = hasReceived(List(msgs), time)

        def hasReceived(msgs: List[TestMsg], time: Int): Boolean = {
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
        val m = new TestMsg
        m.payload = str
        m
    }

    "a channel manager" should {
        "send/recv messages such that" >> {
            "listeners receive one message" in {
                lgr.debug("begin test 1")
                val client = new TestChannelManager
                val server = new TestChannelManager
                server.startListener(8080)
                client.sendMessage(RemoteNode("localhost",8080), "test1") 
                server.hasReceived("test1",10) must_== true
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
                server.hasReceived(msgs.map(string2testmsg(_)),10) must_== true
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
                    val msgs = List("a","b","c","123","456").map("thread"+i+"_"+_).toList
                    allMsgs = allMsgs ::: msgs
                    val t = new Thread {
                        override def run = {
                            msgs.foreach(client.sendMessage(RemoteNode("localhost",8082),_))
                        }
                    }
                    t.start
                })
                server.hasReceived(allMsgs.map(string2testmsg(_)),10) must_== true
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
                server.hasReceived(msgs.map(string2testmsg(_)),10) must_== true
                client.hasReceived(msgs.map(string2testmsg(_)),10) must_== true
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
                server.hasReceived(allMsgs.map(string2testmsg(_)),10) must_== true
                client.hasReceived(allMsgs.map(string2testmsg(_)),10) must_== true
                server.reset
                lgr.debug("end test 5")
            }
        }
    }
}

class ChannelManagerTest extends JUnit4(ChannelManagerSpec)
