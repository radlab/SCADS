import edu.berkeley.cs.scads.comm._
import org.apache.avro.util.Utf8
import scala.actors.Actor._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._
import scala.actors.Actor._

val ser = new StorageEchoServer
ser.startListener(9000)

implicit val c = new StorageActorProxy
val a = actor {
	val id = ActorRegistry.registerActor(self)
	val req = new StorageRequest
	req.src = id

	c.sendMessage(RemoteNode("localhost", 9000), req)
	reactWithin(1000) {
		case (RemoteNode(host, port), msg) => println(msg)
	}
}
