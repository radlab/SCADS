package edu.berkeley.cs
package scads
package piql
package tpcw
package scale

import comm._
import avro.runtime._

class MessagePassingTracer(val traceSink: FileTraceSink) extends MessageHandlerListener {
   def handleEvent(evt: MessageHandlerEvent): MessageHandlerResponse = {
     if (storage.shouldSampleTrace) {
       evt match {
         case MessagePending(_, Left(m)) => traceSink.recordEvent(MessageEvent(m.toBytes))
         case MessagePending(_, Right(m)) => traceSink.recordEvent(MessageEvent(m.toBytes))
       }
    }
     RelayMessage
   }
}
