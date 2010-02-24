import edu.berkeley.cs.scads.comm._
import org.apache.avro.util.Utf8
import scala.actors.Actor._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import scala.actors.Actor._

val ser = new StorageEchoServer
ser.startListener(9000)

val a = actor {
	val id = MessageHandler.registerActor(self)
	val req = new Message
	req.src = id

	MessageHandler.sendMessage(RemoteNode("localhost", 9000), req)
	reactWithin(1000) {
		case (RemoteNode(host, port), msg) => println(msg)
	}
}
