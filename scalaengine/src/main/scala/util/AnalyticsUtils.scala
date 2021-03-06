package edu.berkeley.cs.scads.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData,GenericDatumReader,GenericDatumWriter,IndexedRecord}
import org.apache.avro.io.{DecoderFactory,EncoderFactory}

import edu.berkeley.cs.avro.runtime.ScalaSpecificRecord

import java.io.ByteArrayInputStream

import scala.collection.JavaConversions._
import scala.collection.mutable.StringBuilder
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

import scala.collection.mutable.{ArrayBuilder,HashMap}


trait Filter[R <: ScalaSpecificRecord] extends Serializable {
  def applyFilter(r:R):Boolean
}

// this is a class right now as we need the type parameter
abstract class LocalAggregate[TransType <: ScalaSpecificRecord,
                              ResultType <: Any](implicit transType:scala.reflect.Manifest[TransType]) {
  def init():TransType
  def foldFunction(cur:TransType,next:TransType):TransType
  def finalize(a:TransType):ResultType

  private var curBuilder:ArrayBuilder[TransType] = null
  private val replyMap = new HashMap[GenericData.Record,ArrayBuilder[TransType]]
  def addForGroup(group:GenericData.Record,ar:Array[Byte]) {
    curBuilder = 
      if (group == null) { // not grouping
        if (curBuilder == null) 
          new ArrayBuilder.ofRef[TransType]()
        else
          curBuilder
      } else {
        replyMap.get(group) match {
          case Some(thing) => thing
          case None => new ArrayBuilder.ofRef[TransType]()
        }
      }
    val x = transType.erasure.newInstance().asInstanceOf[TransType]
    x.parse(ar)
    curBuilder += x
    if (group != null)
      replyMap += ((group,curBuilder))
  }

  def finishForGroup(group:GenericData.Record):ResultType = {
    val vls = 
      if (group == null)
        curBuilder
      else
        replyMap(group)
    finalize(vls.result.foldLeft(init())(foldFunction))
  }

  def replyInstance():TransType = {
    transType.erasure.newInstance().asInstanceOf[TransType]
  }

  def groups():Iterable[GenericData.Record] = replyMap.keys
}

trait RemoteAggregate[TransType <: ScalaSpecificRecord,
                KeyType <: ScalaSpecificRecord,
                ValueType <: ScalaSpecificRecord] extends Serializable{
  var stop:Boolean = false
  protected def toDouble(v:Any):Double = {
    v match {
      case x:java.lang.Integer => x.doubleValue
      case x:java.lang.Long => x.doubleValue
      case x:java.lang.Float => x.doubleValue
      case x:java.lang.Double => x.doubleValue
    }
  }
  def init():TransType
  def applyAggregate(a:TransType,k:KeyType,v:ValueType):TransType
}

object AnalyticsUtils {

  val reader = new GenericDatumReader[GenericData.Record]
  val writer = new GenericDatumWriter[Object]

  val groupBuilder = new StringBuilder

  private val shippedLoader = new ShippedClassLoader
  
  def getSubSchema(fields:Seq[String],
                   original:Schema):Schema = {
    val rec = Schema.createRecord(original.getName,
                                  original.getDoc,
                                  original.getNamespace,
                                  original.isError)
    rec.setFields(
      fields map(fn => {
        val of = original.getField(fn)
        new Schema.Field(of.name,
                         of.schema,
                         of.doc,
                         of.defaultValue)
      })
    )
    rec
  }

  // this is gross and slow
  def getGroupBytes(groups:Seq[Int], groupSchemas:Seq[Schema], rec:IndexedRecord):Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream(128)
    val encoder = EncoderFactory.get().binaryEncoder(out,null)
    groups.view.zipWithIndex foreach(ge => {
      writer.setSchema(groupSchemas(ge._2))
      writer.write(rec.get(ge._1),encoder)
    })
    encoder.flush
    out.toByteArray
  }


  def getRecord(filterSchema:Schema,
                bytes:Array[Byte]):GenericData.Record = {
    val inStream = DecoderFactory.get().binaryDecoder(bytes, null) 
    reader.setSchema(filterSchema)
    reader.setExpected(filterSchema)
    reader.read(null,inStream)
  }

  def extractFilterFields(originalSchema:Schema,
                          filterSchema:Schema,
                          bytes:Array[Byte],
                          reuse:GenericData.Record):GenericData.Record = {
    // NB: We offset by 16 here to skip the metadata.  Will need to change if metadata format changes
    val dec = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes,16,(bytes.length-16)),null)
    reader.setSchema(originalSchema)
    reader.setExpected(filterSchema)
    reader.read(reuse,dec)
  }

  /* Groups should be int positions of groups to pull out. */
  def getGroupKey(groups:Seq[Int], rec:IndexedRecord):CharSequence = {
    groupBuilder.clear
    groups.foreach(group => {groupBuilder.append(rec.get(group))})
    groupBuilder.toString
  }

  /* get array of bytes which is the compiled version of cl */
  def getClassBytes(cl:AnyRef):Array[Byte] = {
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
        val name = cl.getClass.getName.replace('.', '/') + ".class"
        val istream = rcl.getResourceAsStream(name)
        if (istream == null) {
          throw new ClassNotFoundException(name)
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

  private class ShippedClassLoader(var bytes:Array[Byte] = null,var targetClass:String = null) extends ClassLoader {
    override def findClass(name:String):Class[_] = {
      if (name.equals(targetClass)) 
        defineClass(name, bytes, 0, bytes.length)
      else 
        Class.forName(name)
    }
  }

  def deserializeCode(name:String, ba:Array[Byte]):Any = {
    try {
      shippedLoader.bytes = ba
      shippedLoader.targetClass = name
      Class.forName(name,false,shippedLoader)
    } catch {
      case ex:java.io.IOException => {
        ex.printStackTrace
        (null,0)
      }
    } 
  }

}
