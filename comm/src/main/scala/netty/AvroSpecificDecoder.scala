package edu.berkeley.cs.scads.comm
package netty

import org.jboss.netty._
import buffer._
import channel._
import handler._

import codec.frame._
import codec.oneone._

import org.apache.avro._
import io._
import specific._

import net.lag.logging.Logger

/**
 * Special vesion of SpecificData allows the user to specify the classloader
 * instead of relying on the classloader used to load SpecificData
 */
class CustomLoaderSpecificData(loader: ClassLoader) extends SpecificData {
  import org.apache.avro.Schema.Type._

  protected val logger = Logger()
  
  private val classCache = new java.util.concurrent.ConcurrentHashMap[String, Class[_]]
  
  val NO_CLASS = new Object(){}.getClass();

  override def getClass(schema: Schema): Class[_] = {
    logger.info("Locating class %s", schema)
    schema.getType match {
      case FIXED | RECORD | ENUM =>
	val name = schema.getFullName();
        if (name == null) return null
        var c = classCache.get(name)
        if (c == null) {
          try {
            c = loader.loadClass(getClassName(schema))
          } catch {
            case e: ClassNotFoundException =>
	      logger.warning("Failed to locate class %s", name)
	      c = NO_CLASS
          }
          classCache.put(name, c)
        }
        return if(c == NO_CLASS) null else c
      case _ => super.getClass(schema)
    }
  }
}

class AvroSpecificDecoder[M <: SpecificRecord](implicit m: Manifest[M])
  extends OneToOneDecoder {

  private val msgClass = m.erasure.asInstanceOf[Class[M]]

  private val schema = msgClass.newInstance.getSchema

  private val msgReader = {
    val constructor = classOf[SpecificDatumReader[M]].getDeclaredConstructor(classOf[org.apache.avro.Schema], classOf[org.apache.avro.Schema], classOf[org.apache.avro.specific.SpecificData])
    constructor.setAccessible(true)
    constructor.newInstance(schema, schema, new CustomLoaderSpecificData(msgClass.getClassLoader))
  }

  private val logger = Logger()

  override def decode(ctx: ChannelHandlerContext, chan: Channel, msg: AnyRef) = msg match {
    case cbuf: ChannelBuffer =>
      val is  = new ChannelBufferInputStream(cbuf)
      val dec = DecoderFactory.get().directBinaryDecoder(is, null)
      val msg = msgClass.newInstance
      msgReader.read(msg, dec)
    case _ => 
      logger.warning("Failed to decode message of unsuported type: %s", msg)
      msg
  }

}
