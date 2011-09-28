package edu.berkeley.cs.scads.comm
package netty

import java.io._

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

import net.lag.logging.Logger
import edu.berkeley.cs.avro.runtime._

class AvroSpecificEncoder[M <: IndexedRecord](implicit schema: TypedSchema[M])
  extends OneToOneEncoder {

  private val msgWriter = 
    new SpecificDatumWriter[M](schema)
  
  private val logger = Logger()

  override def encode(ctx: ChannelHandlerContext, chan: Channel, msg: AnyRef) = msg match {
    case s: SpecificRecord =>
      val os  = new ExposingByteArrayOutputStream(512)
      val enc = EncoderFactory.get().binaryEncoder(os,null)
      // TODO: fix unsafe cast here
      msgWriter.write(msg.asInstanceOf[M], enc)
      enc.flush
      ChannelBuffers.wrappedBuffer(os.getUnderlying, 0, os.size)
    case _ => 
      logger.warning("Failed to encode message of unsupported type: %s", msg)
      msg
  }

}
