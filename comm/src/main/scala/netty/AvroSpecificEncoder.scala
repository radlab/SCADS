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
import io._
import specific._

class AvroSpecificEncoder[M <: SpecificRecord](implicit m: Manifest[M])
  extends OneToOneEncoder {

  private val msgClass = m.erasure.asInstanceOf[Class[M]]

  private val msgWriter = 
    new SpecificDatumWriter[M](msgClass.newInstance.getSchema)
  
  override def encode(ctx: ChannelHandlerContext, chan: Channel, msg: AnyRef) = msg match {
    case s: SpecificRecord =>
      val os  = new ExposingByteArrayOutputStream(512)
      val enc = new BinaryEncoder(os)
      // TODO: fix unsafe cast here
      msgWriter.write(msg.asInstanceOf[M], enc)
      ChannelBuffers.wrappedBuffer(os.getUnderlying, 0, os.size)
    case _ => 
      /** TODO: log a message */
      msg
  }

}
