import org.apache.log4j.Logger
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}


val storageNodes = Array(r10,r11,r6,r13)
val dataPlacementNode = r15

(storageNodes ++ Array(dataPlacementNode)).foreach(_.services.foreach(_.stop))

storageNodes.foreach((n)=> {
    n.setupRunit
    val storageNodeService = new JavaService(
            "../scalaengine/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.storage.JavaEngine","")
    storageNodeService.action(n)
    n.services.foreach((s) => {
            println(s) 
            s.watchLog
            s.start
            println(s.status) 
    })
})

val dataPlacementNodeService = new JavaService(
        "../placement/placement-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.placement.SimpleDataPlacementApp","")
dataPlacementNode.setupRunit
dataPlacementNodeService.action(dataPlacementNode)
dataPlacementNode.services.foreach((s) => {
            println(s) 
            s.watchLog
            s.start
            println(s.status) 
    })

// try to obtain connection to data placement node
val dpclient = getDataPlacementHandle("r15.millennium.berkeley.edu",false)

val range1 = new RangeSet();
val rs1 = new RecordSet(3,range1,null,null)

(storageNodes ++ Array(dataPlacementNode)).foreach(_.services.foreach(_.stop))

def getDataPlacementHandle(h:String,xtrace_on:Boolean):KnobbedDataPlacementServer.Client = { 
    val p = 8000 // default port
    var haveDPHandle = false
    var dpclient:KnobbedDataPlacementServer.Client = null
    while (!haveDPHandle) {
        try {
            val transport = new TFramedTransport(new TSocket(h, p)) 
            val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
            dpclient = new KnobbedDataPlacementServer.Client(protocol)
            transport.open()
            haveDPHandle = true
        } catch {
            case e: Exception => { println("don't have connection to placement server, waiting 1 second: " + e.getMessage); e.printStackTrace; Thread.sleep(1000) }
        }   
    }   
    dpclient
}   
