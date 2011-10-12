package edu.berkeley.cs
package scads
package piql
package modeling

import comm._
import avro.runtime._

class MessagePassingTracer(val traceSink: FileTraceSink) extends MessageHandlerListener {
   def handleEvent(evt: MessageHandlerEvent): MessageHandlerResponse = {
     evt match {
       case MessagePending(_, Left(m)) => traceSink.recordEvent(MessageEvent(m.toBytes))
       case MessagePending(_, Right(m)) => traceSink.recordEvent(MessageEvent(m.toBytes))
     }
     RelayMessage
   }
}
