package edu.berkeley.cs
package scads
package comm
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

class AvroSpecificDecoder[M <: SpecificRecord](implicit m: Manifest[M])
  extends OneToOneDecoder {

  private val msgClass = m.erasure.asInstanceOf[Class[M]]

  private val schema = msgClass.newInstance.getSchema

  private val msgReader = new SpecificDatumReader[M](schema, schema, new CustomLoaderSpecificData(msgClass.getClassLoader))
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
