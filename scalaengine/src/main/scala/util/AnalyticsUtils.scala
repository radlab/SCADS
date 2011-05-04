package edu.berkeley.cs.scads.util

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData,GenericDatumReader}
import org.apache.avro.io.{BinaryDecoder,DecoderFactory}

import java.io.ByteArrayInputStream

import scala.collection.JavaConversions._
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io.AbstractFile

object AnalyticsUtils {

  val decoderFactory = new DecoderFactory()
  val decoder:BinaryDecoder = null
  val reader = new GenericDatumReader[GenericData.Record]()
  
  def getFilterSchema(original:Schema,
                      fields:Seq[String]):Schema = {
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

  def getFilterRecord(filterSchema:Schema,
                      bytes:Array[Byte]):GenericData.Record = {
    val dec = decoderFactory.createBinaryDecoder(new ByteArrayInputStream(bytes),decoder)
    reader.setSchema(filterSchema)
    reader.setExpected(filterSchema)
    reader.read(null,dec)
  }

  def extractFilterFields(originalSchema:Schema,
                          filterSchema:Schema,
                          bytes:Array[Byte],
                          reuse:GenericData.Record):GenericData.Record = {
    // NB: We offset by 16 here to skip the metadata.  Will need to change if metadata format changes
    val dec = decoderFactory.createBinaryDecoder(new ByteArrayInputStream(bytes,16,(bytes.length-16)),decoder)
    reader.setSchema(originalSchema)
    reader.setExpected(filterSchema)
    reader.read(reuse,dec)
  }


  /* get array of bytes which is the compiled version of cl */
  def getFunctionCode(cl:AnyRef):Array[Byte] = {
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

  private class ShippedClassLoader(ba:Array[Byte],targetClass:String) extends ClassLoader {
    override def findClass(name:String):Class[_] = {
      if (name.equals(targetClass))
        defineClass(name, ba, 0, ba.length)
      else 
        Class.forName(name)
    }
  }

  def deserializeCode(name:String, ba:Array[Byte]):Any = {
    try {
      val loader = new ShippedClassLoader(ba,name)
      Class.forName(name,false,loader)
    } catch {
      case ex:java.io.IOException => {
        ex.printStackTrace
        (null,0)
      }
    } 
  }

}
