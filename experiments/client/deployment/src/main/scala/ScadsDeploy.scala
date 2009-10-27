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

class ScadsDeploy(storageNodes: Array[Tuple2[RClusterNode,Int]], dataPlacementNode: Tuple2[RClusterNode,Int])  {

    val logger = Logger.getLogger("ScadsDeploy")
    var debugLevel = Level.DEBUG
    var maxBlockingTries = 1000
    private val allNodes = storageNodes ++ Array(dataPlacementNode)

    def shutdown():Unit = {
        allNodes.foreach(_._1.cleanServices)
    }

    def deploy():Unit = {

        // Set up the remote logger
        val remoteLogger = Logger.getLogger("deploylib.remoteMachine")
        remoteLogger.setLevel(debugLevel)

        // Iterate over all the storage + dataplacement nodes to
        // clean up any pre-existing services, so we start fresh
        allNodes.foreach(_._1.cleanServices)

        // Give the services a chance to clean
        // TODO: we really need to have blocking command execution, this
        // is not the right way to do this
        Thread.sleep(1000)

        // Check to see if the port is open on the remote machine
        // for each node
        allNodes.foreach((tuple) => {
            if ( !tuple._1.isPortAvailableToListen(tuple._2) ) {
                val msg = "Port " + tuple._2 + " is in use on " + tuple._1
                logger.fatal(msg)
                throw new RemotePortInUseException(msg)
            }
        })

        // Start the storage engine service on all the storage nodes
        storageNodes.foreach( (tuple) => {
            val rnode = tuple._1
            val port = tuple._2
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
