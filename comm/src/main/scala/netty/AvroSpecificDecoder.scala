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
import generic.IndexedRecord
import io._
import specific._
import avro.runtime._

import net.lag.logging.Logger

class AvroSpecificDecoder[M <: IndexedRecord](classLoader: ClassLoader)(implicit schema: TypedSchema[M])
  extends OneToOneDecoder {

  private val msgReader = new SpecificDatumReader[M](schema, schema, new SpecificData(classLoader))

  private val logger = Logger()

  override def decode(ctx: ChannelHandlerContext, chan: Channel, msg: AnyRef) = msg match {
    case cbuf: ChannelBuffer =>
      val is  = new ChannelBufferInputStream(cbuf)
      val dec = DecoderFactory.get().directBinaryDecoder(is, null)
      msgReader.read(null.asInstanceOf[M], dec).asInstanceOf[M]
    case _ => 
      logger.warning("Failed to decode message of unsuported type: %s", msg)
      msg
  }
}
