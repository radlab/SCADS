import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Actors._
import scala.actors._
import scala.actors.Actor._
import scala.concurrent.SyncVar

val handle = new SyncVar[Array[Byte]]

remoteActor((ra) => {
  handle.set(ra.toBytes)
  receive {
    case EmptyResponse() => {println("got ER") ;reply(EmptyResponse())}
  }
})

val remoteHandle = new RemoteActor().parse(handle.get)
println(remoteHandle !? EmptyResponse())
