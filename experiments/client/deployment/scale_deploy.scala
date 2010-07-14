import edu.berkeley.cs.scads.deployment.ScadsDeploy

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

val logger = Logger.getLogger("scaleDeploy")
logger.setLevel(Level.DEBUG)

val storageMachines = List( (r11,9003), (r12,9003), (r13,9003), (r15,9003) ).map( (t) => StorageMachine.fromTuple(t) )
val dataPlacementMachine = DataPlacementMachine.fromTuple( (r10,8002) )

val scadsDeploy = new ScadsDeploy2(storageMachines, dataPlacementMachine, 1)

scadsDeploy.deploy(true) // TODO: don't clean services, so data persists


// add a dummy user to set the default namespaces
implicit val env:Environment = scadsDeploy.getEnv
val user = new user
user.name("__DUMMY__")
user.save


val clientMachines = List( r16, r17, r18 )

val numClientThreads = 10
val numUsersPerClientThread = (150000*storageMachines.size)/clientMachines.size

clientMachines.zip(0 until clientMachines.size).foreach( (rnode,i) => {

    val argBuf = new StringBuffer
    argBuf.append("-p ")
    argBuf.append(dataPlacementMachine.thriftPort)
    argBuf.append("-h ")
    argBuf.append(dataPlacementMachine.machine.hostname)
    argBuf.append("-c ")
    argBuf.append(i)
    argBuf.append("-n ")
    argBuf.append(numClientThreads)
    argBuf.append("-u ")
    argBuf.append(numUsersPerClientThread)
    argBuf.append("-s ")
    argBuf.append(15) // load 15% at first
    argBuf.append("-m before")

    val clientService = new JavaService("target/clientdeployment-1.0-SNAPSHOT-jar-with-dependencies.jar","DataLoadProcess",argBuf.toString, "-Xms512M -Xmx1024M")
    clientService.action(rnode)
    rnode.services(0).watchLog
    rnode.services(0).once // only want to load data once
    ScadsDeployUtil.blockUntilRunning(rnode.services(0))

})

// now wait until all of the clients have finished loading data
clientMachines.foreach( (rnode) => {
    val clientService = rnode.services(0)
    while (clientService.status.trim.equals("run")) {
        logger.debug(rnode + " is still running the before data load...")
        Thread.sleep(60000L) // wait a minute, check again.
    }
})

// now rebalance
scadsDeploy.rebalance


// now redeploy the rest of the data
clientMachines.zip(0 until clientMachines.size).foreach( (rnode,i) => {

    val argBuf = new StringBuffer
    argBuf.append("-p ")
    argBuf.append(dataPlacementMachine.thriftPort)
    argBuf.append("-h ")
    argBuf.append(dataPlacementMachine.machine.hostname)
    argBuf.append("-c ")
    argBuf.append(i)
    argBuf.append("-n ")
    argBuf.append(numClientThreads)
    argBuf.append("-u ")
    argBuf.append(numUsersPerClientThread)
    argBuf.append("-s ")
    argBuf.append(15) // load 15% at first
    argBuf.append("-m after")

    val clientService = new JavaService("target/clientdeployment-1.0-SNAPSHOT-jar-with-dependencies.jar","DataLoadProcess",argBuf.toString, "-Xms512M -Xmx1024M")
    clientService.action(rnode)
    rnode.services(1).watchLog
    rnode.services(1).once // only want to load data once
    ScadsDeployUtil.blockUntilRunning(rnode.services(1))
})

// now wait until all of the clients have finished loading data
clientMachines.foreach( (rnode) => {
    val clientService = rnode.services(1)
    while (clientService.status.trim.equals("run")) {
        logger.debug(rnode + " is still running the after data load...")
        Thread.sleep(60000L) // wait a minute, check again.
    }
})
