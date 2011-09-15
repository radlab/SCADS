package edu.berkeley.cs
package scads
package piql
package modeling

import comm._

class MessagePassingTracer(val traceSink: FileTraceSink) extends MessageHandlerListener[Message, Message] {
   def handleEvent(evt: MessageHandlerEvent[Message, Message]): MessageHandlerResponse = {
     evt match {
       case MessagePending(_, Left(m)) => traceSink.recordEvent(MessageEvent(m))
       case MessagePending(_, Right(m)) => traceSink.recordEvent(MessageEvent(m))
     }
     RelayMessage
   }
}
