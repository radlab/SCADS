package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

case class RemotePortInUseException(msg: String) extends Exception
case class BlockingTriesExceededException(msg: String) extends Exception

case class RemoteDataPlacement(host: String, port: Int, logger: Logger) extends RemoteDataPlacementProvider

class ScadsDeploy(storageNodes: scala.collection.immutable.Map[RClusterNode,Int], dataPlacementNode: Tuple2[RClusterNode,Int])  {

    val logger = Logger.getLogger("ScadsDeploy")
    var debugLevel = Level.DEBUG
    var maxBlockingTries = 1000
    private val allNodes = storageNodes + dataPlacementNode

    private def stopAllServices(): Unit = {
        allNodes.keySet.foreach(_.services.foreach(_.stop))
        allNodes.keySet.foreach(_.cleanServices)
    }

    def shutdown():Unit = {
        stopAllServices 
    }

    def deploy():Unit = {

        // Set up the remote logger
        val remoteLogger = Logger.getLogger("deploylib.remoteMachine")
        remoteLogger.setLevel(debugLevel)

        // Iterate over all the storage + dataplacement nodes to
        // clean up any pre-existing services, so we start fresh
        stopAllServices

        // Give the services a chance to clean
        // TODO: we really need to have blocking command execution, this
        // is not the right way to do this
        Thread.sleep(1000)

        // Check to see if the port is open on the remote machine
        // for each node
        allNodes.keySet.foreach((node) => {
            if ( !node.isPortAvailableToListen(allNodes(node)) ) {
                val msg = "Port " + allNodes(node) + " is in use on " + node 
                logger.fatal(msg)
                throw new RemotePortInUseException(msg)
            }
        })

        // Start the storage engine service on all the storage nodes
        storageNodes.keySet.foreach( (rnode) => {
            val port = storageNodes(rnode) 
            // Setup runit on the node
            rnode.setupRunit
            val storageNodeService = new JavaService(
                "../../../scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.storage.JavaEngine","-p " +port)
            storageNodeService.action(rnode)
            rnode.services(0).watchLog
            rnode.services(0).start
            blockUntilRunning(rnode.services(0))
        })

        // Start up the data placement node
        val rnode = dataPlacementNode._1
        val port = dataPlacementNode._2
        val dataPlacementNodeService = new JavaService(
            "../../../placement/target/placement-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.placement.SimpleDataPlacementApp",port.toString)
        dataPlacementNodeService.action(rnode)
        rnode.services(0).watchLog
        rnode.services(0).start
        blockUntilRunning(rnode.services(0))

    }

    def placeUsers(userPlacement: Array[Tuple3[String,String,RClusterNode]]):Unit = {
        val dpclient = getDataPlacementHandle(dataPlacementNode._2,dataPlacementNode._1.hostname,false)
        userPlacement.foreach( (tuple) => {
            val from = tuple._1
            val to = tuple._2
            val rnode = tuple._3
            
            val range = new RangeSet()
            val fromField = new StringField
            fromField.value = from
            val toField = new StringField
            toField.value = to
            range.setStart_key(fromField.serializeKey)
            range.setEnd_key(toField.serializeKey)
            val rs = new RecordSet(3,range,null,null)


            val dp = new DataPlacement(rnode.hostname,storageNodes(rnode),storageNodes(rnode),rs)
            val ll = new java.util.LinkedList[DataPlacement]
            ll.add(dp)
            dpclient.add("ent_user", ll)
        })
    } 

    private def getDataPlacementHandle(p: Int, h:String,xtrace_on:Boolean):KnobbedDataPlacementServer.Client = {
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

    def getEnv():Environment = {
        implicit val env = new Environment
        env.placement = new RemoteDataPlacement(dataPlacementNode._1.hostname,dataPlacementNode._2,logger)
        env.session = new TrivialSession
        env.executor = new TrivialExecutor
        env
    }

    private def blockUntilRunning(runitService: Service):Unit = {
        var i = 0
        while( !runitService.status.trim.equals("run") ) {
            if ( i == maxBlockingTries ) {
                val msg = "Exceeded max blocking tries"
                logger.fatal(msg)
                throw new BlockingTriesExceededException(msg)
            }
            logger.info("got status '" + runitService.status + "', expecting 'run'")
            runitService.start // keep trying!
            Thread.sleep(1000);// try to mitigate busy-wait
            i += 1
        }
    }


}
