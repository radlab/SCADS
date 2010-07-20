package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.comm._

import scala.actors._
import scala.actors.Actor._
import java.util.Arrays

import scala.collection.mutable.HashMap
import scala.concurrent.SyncVar
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import org.apache.log4j.Logger

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData.{Array => AvroArray}
import org.apache.avro.generic.GenericData
import org.apache.avro.io.BinaryData
import org.apache.avro.io.DecoderFactory
import com.googlecode.avro.runtime.ScalaSpecificRecord

import org.apache.zookeeper.CreateMode

trait RemoteExecution[KeyType <: ScalaSpecificRecord, ValueType <: ScalaSpecificRecord] extends Namespace[KeyType, ValueType] {
  protected val keyClass: Class[_]
  protected val valueClass: Class[_]

  def flatMap[RetType <: ScalaSpecificRecord](func: (KeyType, ValueType) => List[RetType])(implicit retType: scala.reflect.Manifest[RetType]): Seq[RetType] = {
    class ResultSeq extends Actor with Seq[RetType] {
      val retClass = retType.erasure
      val result = new SyncVar[List[RetType]]

      def apply(ordinal: Int): RetType = result.get.apply(ordinal)
      def length: Int = result.get.length
      override def elements: Iterator[RetType] = result.get.iterator
      override def iterator: Iterator[RetType] = elements

      def act(): Unit = {
        val id = MessageHandler.registerActor(self)
        val ranges = splitRange(null, null)
        var remaining = 0 /** Use ranges.size once we upgrade to RC2 or greater */
        var partialElements = List[RetType]()
        //val msg = new Message
        //val req = new FlatMapRequest
        //msg.src = new java.lang.Long(id)
        //msg.dest = dest
        //msg.body = req
        //req.namespace = namespace
        //req.keyType = keyClass.getName
        //req.valueType = valueClass.getName
        //req.codename = func.getClass.getName
        //req.closure = getFunctionCode(func)

        val msg = Message(
                ActorNumber(id),
                dest,
                null,
                FlatMapRequest(
                    namespace,
                    keyClass.getName,
                    valueClass.getName,
                    func.getClass.getName,
                    getFunctionCode(func)))

        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.head, msg)
          remaining += 1
        })

        loop {
          reactWithin(timeout) {
            case (_, msg: Message) => {
              val resp = msg.body.asInstanceOf[FlatMapResponse]

              //TODO: Use scala idioms for iteration
              //val records = resp.records.iterator()
              //while(records.hasNext) {
              //  val rec = retClass.newInstance.asInstanceOf[RetType]
              //  rec.parse(records.next)
              //  partialElements ++= List(rec)
              //}
              resp.records.foreach( b => {
                  val rec = retClass.newInstance.asInstanceOf[RetType]
                  rec.parse(b)
                  partialElements ++= List(rec)
              })


              remaining -= 1
              if(remaining < 0) { // count starts a 0
                result.set(partialElements)
                MessageHandler.unregisterActor(id)
              }
            }
            case TIMEOUT => result.set(null)
            case m => logger.fatal("Unexpected message: " + m)
          }
        }
      }
    }

    (new ResultSeq).start.asInstanceOf[Seq[RetType]]
  }

  def filter(func: (KeyType, ValueType) => Boolean): Iterable[(KeyType,ValueType)] = {
    val result = new SyncVar[List[(KeyType,ValueType)]]
    var list = List[(KeyType,ValueType)]()
    val ranges = splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FilterRequest
      //msg.src = new java.lang.Long(id)
      msg.src = ActorNumber(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.head, msg)
        remaining += 1
      })

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val resp = msg.body.asInstanceOf[RecordSet]
            val recit = resp.records.iterator
            while (recit.hasNext) {
              val rec = recit.next
              list = list:::List((deserializeKey(rec.key),deserializeValue(rec.value)))
            }
            remaining -= 1
            if(remaining <= 0) {
              result.set(list)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for filter to return")
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message in filter: " + m)
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    val rlist = result.get
    if (rlist == null)
      null
    else
      rlist
  }

  def foldLeft(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType)): (KeyType,ValueType) =
    doFold(z)(func,0)

  def foldRight(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType)): (KeyType,ValueType) =
    doFold(z)(func,1)

  private def doFold(z:(KeyType,ValueType))(func: ((KeyType,ValueType),(KeyType,ValueType)) => (KeyType,ValueType),dir:Int): (KeyType,ValueType) = {
    var list = List[(KeyType,ValueType)]()
    val result = new SyncVar[List[(KeyType,ValueType)]]
    val ranges = splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      val msg = new Message
      val fr = new FoldRequest
      msg.src = ActorNumber(id)
      msg.dest = dest
      msg.body = fr
      fr.namespace = namespace
      fr.keyType = keyClass.getName
      fr.valueType = valueClass.getName
      fr.initValueOne = serializeKey(z._1)
      fr.initValueTwo = serializeValue(z._2)
      fr.codename = func.getClass.getName
      fr.code = getFunctionCode(func)
      fr.direction = dir // TODO: Change dir to an enum when we support that
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      ranges.foreach(r => {
        MessageHandler.sendMessage(r.nodes.head, msg)
        remaining += 1
      })

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val rec = msg.body.asInstanceOf[Record]
            list ++= List((deserializeKey(rec.key),deserializeValue(rec.value)))
            remaining -= 1
            if(remaining <= 0) {
              result.set(list)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for foldLeft to return")
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message in foldLeft: " + m)
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    val rlist = result.get
    if (rlist == null)
      z
    else
      if (dir == 0)
        rlist.foldLeft(z)(func)
      else
        rlist.foldRight(z)(func)
  }

  def foldLeft2L[TypeRemote <: ScalaSpecificRecord,TypeLocal](remInit:TypeRemote,localInit:TypeLocal)
                                             (remFunc: (TypeRemote,(KeyType,ValueType)) => TypeRemote,
                                              localFunc: (TypeLocal,TypeRemote) => TypeLocal): TypeLocal = {
    var list = List[TypeRemote]()
    val result = new SyncVar[List[TypeRemote]]
    val ranges = splitRange(null, null)
    actor {
      val id = MessageHandler.registerActor(self)
      var remaining = 0 /** use ranges.size once we upgrade to RC2 */
      try {
        val msg = new Message
        val fr = new FoldRequest2L
        msg.src = ActorNumber(id)
        msg.dest = dest
        msg.body = fr
        fr.namespace = namespace
        fr.keyType = keyClass.getName
        fr.valueType = valueClass.getName
        fr.initType = remInit.getClass.getName
        fr.initValue = remInit.toBytes
        fr.codename = remFunc.getClass.getName
        fr.code = getFunctionCode(remFunc)
        fr.direction = 0 // TODO: Change dir to an enum when we support that
        ranges.foreach(r => {
          MessageHandler.sendMessage(r.nodes.head, msg)
          remaining += 1
        })
      } catch {
        case t:Throwable => {
          println("Some error processing foldLeft2L")
          t.printStackTrace()
          result.set(null)
          MessageHandler.unregisterActor(id)
          exit
        }
      }

      loop {
        reactWithin(timeout) {
          case (_, msg: Message) => {
            val rep = msg.body.asInstanceOf[Fold2Reply]
            val repv = remInit.getClass.newInstance.asInstanceOf[TypeRemote]
            repv.parse(rep.reply)
            list = repv :: list
            remaining -= 1
            if(remaining <= 0) {
              result.set(list)
              MessageHandler.unregisterActor(id)
              exit
            }
          }
          case TIMEOUT => {
            logger.warn("Timeout waiting for foldLeft to return")
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
          case m => {
            logger.fatal("Unexpected message in foldLeft: " + m)
            result.set(null)
            MessageHandler.unregisterActor(id)
            exit
          }
        }
      }
    }
    val rlist = result.get
    if (rlist == null)
      localInit
    else
      rlist.foldLeft(localInit)(localFunc)
  }

  /* get array of bytes which is the compiled version of cl */
  private def getFunctionCode(cl:AnyRef):Array[Byte] = {
    val ldr = cl.getClass.getClassLoader
    ldr match {
      case afcl:AbstractFileClassLoader => {
        // can't use getResourceAsStream here because it always returns null on an AFCL
        val name = cl.getClass.getName
        val rootField = afcl.getClass.getDeclaredField("root")
        rootField.setAccessible(true)
        var file: AbstractFile = rootField.get(afcl).asInstanceOf[AbstractFile]
        val pathParts = name.split("[./]").toList
        for (dirPart <- pathParts.init) {
          file = file.lookupName(dirPart, true)
            if (file == null) {
              throw new ClassNotFoundException(name)
            }
        }
        file = file.lookupName(pathParts.last+".class", false)
        if (file == null) {
          throw new ClassNotFoundException(name)
        }
        file.toByteArray
      }
      case rcl:ClassLoader => {
        val name = cl.getClass.getName.replaceAll("\\.", "/")+".class"
        val istream = cl.getClass.getResourceAsStream(name)
        if (istream == null) {
          logger.error("Couldn't find stream for class")
          null
        } else {
          val buf = new Array[Byte](1024)
          val os = new java.io.ByteArrayOutputStream(1024)
          var br = istream.read(buf)
          while(br >= 0) {
            os.write(buf,0,br)
            br = istream.read(buf)
          }
          os.toByteArray
        }
      }
    }
  }

}
