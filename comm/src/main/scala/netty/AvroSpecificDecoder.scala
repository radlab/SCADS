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

class AvroSpecificDecoder[M <: SpecificRecord](implicit m: Manifest[M])
  extends OneToOneDecoder {

  private val msgClass = m.erasure.asInstanceOf[Class[M]]

  private val msgReader = 
    new SpecificDatumReader[M](msgClass.newInstance.getSchema)

  private val decoderFactory = new DecoderFactory
  decoderFactory.configureDirectDecoder(true)

  override def decode(ctx: ChannelHandlerContext, chan: Channel, msg: AnyRef) = msg match {
    case cbuf: ChannelBuffer =>
      val is  = new ChannelBufferInputStream(cbuf)
      val dec = decoderFactory.createBinaryDecoder(is, null) 
      val msg = msgClass.newInstance
      msgReader.read(msg, dec)
    case _ => 
      /** TODO: log a message */
      msg
  }

}
