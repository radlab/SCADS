
import edu.berkeley.cs.scads.comm._
import org.apache.avro.util._
import java.nio.ByteBuffer
import java.util.concurrent.Semaphore


import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.TIMEOUT

import org.apache.log4j.BasicConfigurator


object PerfActorEchoReceiver {
  def main(args: Array[String]): Unit = {
    val ser = new StorageEchoPrintServer
    ser.startListener(9000)
  }
}

object PerfActorEchoSender {
  var dest: RemoteNode = null
  val lock = new Object
  var msgs = 0
  val testSize = 1000000
  val numActors = 5

  class RecvActor extends Actor {
    def act() {
      loop {
        react {
          case (RemoteNode(host, port), msg) => {
            lock.synchronized {
              msgs += 1
              if (msgs % 100000 == 0) println(msgs)
              if (msgs == testSize) lock.notify
            }
          }
        }        
      }
    }
  }

  class SendActor(id:Long) extends Actor {
    //val did = new java.lang.Long(id)
    def act() {
      (1 to testSize/numActors).foreach( j => {
        val req = classOf[Message].newInstance() //HACK
        //req.src = new java.lang.Long(id)
        req.src = ActorNumber(id)
        req.dest = ActorNumber(id)
        req.body = null
        MessageHandler.sendMessage(dest, req)
      })
    }
  }


  // this just seems to hang after about sending
  // 4 million messages. i cannot figure out why
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure
    dest = RemoteNode(args(0), 9000)
    // still need this since we're not registering the send actors
    val refSendArray = new Array[Actor](numActors)
    val idRecvArray = new Array[Long](numActors)
    (1 to 10).foreach(t => {
      val start = System.currentTimeMillis()
      (1 to numActors).foreach( i => {
        val ra = new RecvActor
        val rid = MessageHandler.registerActor(ra)
        idRecvArray(i-1) = rid
        ra.start
        refSendArray(i-1) = (new SendActor(rid)).start
      })
      lock.synchronized { 
        while (msgs < testSize) lock.wait
      }
      val end = System.currentTimeMillis()
      println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
      msgs = 0
      idRecvArray.foreach(id => {
        MessageHandler.unregisterActor(id)
      })
    })
  }

}

object PerfMultiActorEchoSender {
  var dest: RemoteNode = null
  val lock = new Object
  var msgs = 0
  var lim = 1000
  var testSize = 1000000

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure
    dest = RemoteNode(args(0), 9000)
    testSize = Integer.parseInt(args(1))
    val lim = Integer.parseInt(args(2))
    val sem = new Semaphore(lim)
    (1 to 10).foreach(t => {
      val start = System.currentTimeMillis()
      (1 to testSize).foreach( i => {
        sem.acquire
        actor {
          val idl = MessageHandler.registerActor(self)
          //val id = new java.lang.Long(idl)
          val req = classOf[Message].newInstance() //HACK
          req.src = ActorNumber(idl)
          req.dest = ActorNumber(idl)
          req.body = null
          MessageHandler.sendMessage(dest, req)
          react {
            case (RemoteNode(host, port), msg) => {
              lock.synchronized {
                msgs += 1
                if (msgs % 100000 == 0) println(msgs)
                if (msgs == testSize) lock.notify
              }
              sem.release
              MessageHandler.unregisterActor(idl)
            }
            case _ => {
              println("Fallthrough")
              sem.release
              MessageHandler.unregisterActor(idl)
            }
          }
        }
      })
      lock.synchronized { 
        while (msgs < testSize) lock.wait
      }
      val end = System.currentTimeMillis()
      println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
      msgs = 0
    })
  }
}


object PerfActorReceiver {
  def main(args: Array[String]): Unit = {
    val ser = new StorageDiscardServer
    ser.startListener(9000)
  }
}

object PerfActorSender {

  /**
  * 210703.75052675937req/sec
  * 429737.8599054577req/sec
  * 491400.4914004914req/sec
  * 452284.03437358665req/sec
  * 461467.4665436087req/sec
  * 483325.2779120348req/sec
  * 504286.4346949067req/sec
  * 476644.42326024786req/sec
  * 503018.1086519115req/sec
  * 512820.5128205128req/sec
  */
  def main(args: Array[String]): Unit = {
    val testSize = 1000000
    val dest = RemoteNode(args(0), 9000)

    val lock = new Object
    var numFinished = 0

    (1 to 10).foreach(t => {
      val start = System.currentTimeMillis()
      (1 to 5).foreach(i => {
        actor {
          val id = MessageHandler.registerActor(self)
          (1 to testSize/5).foreach(j => {
            val req = classOf[Message].newInstance() //HACK
            //req.src = new java.lang.Long(15)
            req.src = ActorNumber(15L)
            req.body = null
            MessageHandler.sendMessage(dest, req)
          })
          lock.synchronized {
            numFinished += 1
            lock.notify
          }
          MessageHandler.unregisterActor(id)
        }
      })
      lock.synchronized {
        while (numFinished < 5) lock.wait
      }
      val end = System.currentTimeMillis()
      println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
      numFinished = 0
    })
  }
}

object PerfSenderTrivial {

  /**
  * Results on my home desktop, running under scala:console
  * 
  * 69715.56051310652req/sec
  * 98921.75289346128req/sec
  * 105307.49789385004req/sec
  * 108896.87465969726req/sec
  * 90925.62284051646req/sec
  * 101040.7194099222req/sec
  * 105965.8789869662req/sec
  * 99295.0054612253req/sec
  * 101801.89351521937req/sec
  * 98531.87506158242req/sec
  */
  def main(args: Array[String]): Unit = {
    val testSize = 1000000
    val mgr = new DiscardAvroChannelManager[Message, Message]
    val dest = RemoteNode(args(0), 9000)

    (1 to 10).foreach(t => {
      val start = System.currentTimeMillis()
      (1 to testSize).foreach(i => {
        val req = classOf[Message].newInstance() //HACK
        //req.src = new java.lang.Long(15)
        req.src = ActorNumber(15L)
        req.body = null
        mgr.sendMessage(dest, req)
      })
      val end = System.currentTimeMillis()
      println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
    })
  }
}


object PerfSender {

  /**
   * 60580.35984733749req/sec
   * 68927.488282327req/sec
   * 76411.70627340108req/sec
   * 63959.06619763352req/sec
   * 64271.482743106884req/sec
   * 66555.7404326123req/sec
   * 73480.7847747814req/sec
   * 64008.193048710236req/sec
   * 63516.260162601626req/sec
   * 70511.91651389084req/sec
   */
  def main(args: Array[String]): Unit = {
    val testSize = 1000000
    val mgr = new DiscardAvroChannelManager[Message, Message]
    val dest = RemoteNode(args(0), 9000)

    (1 to 10).foreach(t => {
      val start = System.currentTimeMillis()
      (1 to testSize).foreach(i => {
        val pr = classOf[PutRequest].newInstance() 
        pr.namespace = "__namespace__"
        pr.key = "__key__".getBytes
        pr.value = Some("__value__".getBytes)

        val req = classOf[Message].newInstance() //HACK
        //req.src = new java.lang.Long(15)
        req.src = ActorNumber(15L)
        req.body = pr

        mgr.sendMessage(dest, req)
      })
      val end = System.currentTimeMillis()
      println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
    })
  }
}

object PerfReceiver {
  def main(args: Array[String]): Unit = {
    val mgr = new DiscardAvroChannelManager[Message, Message]
    mgr.startListener(9000)
  }
}
