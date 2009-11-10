//package edu.berkeley.cs.scads.deployment

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.TestCluster

import java.util.Random

import java.io.{File,FileNotFoundException}

import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.log4j.Logger

class ClientProcess(val dphost:String, val dpport:Int, val clientId:Int, val numThreads:Int) {

    class WorkerThread(val clientId:Int, val threadId:Int, val env:Environment, val fn:(Int,Int,Environment)=>Unit) extends Runnable {
        override def run():Unit = {
            fn(clientId,threadId,env)
            synchronized {
                threadsCompleted += 1
                if ( threadsCompleted == numThreads ) {
                    println("All threads done, shutting down")
                    System.exit(0)
                }
            }
        }
    }


    val threadPool = new Array[Thread](numThreads)
    val threadPoolMonitor = new Array[File](numThreads)

    private var threadsCompleted = 0

    def initialize(fn:(Int,Int,Environment) => Unit):Unit = {
        for ( i <- 0 until numThreads ) {
            val worker = new WorkerThread(clientId,i,getEnv,fn)
            threadPool(i) = new Thread(worker)
        }
    }

    def start():Unit = {
        threadPool.foreach(_.start)
    }

    private var env:Environment = null

    private def getEnv():Environment = {
        if ( env == null ) {
            implicit val ienv = new Environment
            //env.placement = new RemoteDataPlacementProviderImpl(dphost,dpport)
            ienv.placement = new TestCluster
            ienv.session = new TrivialSession
            ienv.executor = new TrivialExecutor
            env = ienv
        }
        env
    }
    

}

case class RemoteDataPlacementProviderImpl(host:String,port:Int) extends TransparentRemoteDataPlacementProvider {
    val logger = Logger.getLogger("scads.remoteDP")
}

object DataLoadProcess {

    val logger = Logger.getLogger("DataLoadProcess")

    def main(args:Array[String]):Unit = {
        println("Initializing")
        logger.debug("Initializing DataLoadProcess...")

		val options = new Options
        options.addOption("p", "dpport", true, "the port of the DP server")
        options.addOption("h", "dphost", true, "the host of the DP server")

        options.addOption("c", "clientid", true, "the client ID of this client")
        options.addOption("n", "numthreads", true, "the number of threads of this client")
        options.addOption("u", "numusers", true, "the number of users to load")
        options.addOption("s", "stoppoint", true, "the stop point in percentage")
        options.addOption("m", "mode", true, "before or after stop point")

		val parser = new GnuParser
		val cmd = parser.parse( options, args)

        val port = cmd.getOptionValue("dpport").toInt
        val host = cmd.getOptionValue("dphost")

        val clientId = cmd.getOptionValue("clientid").toInt
        val numThreads = cmd.getOptionValue("numthreads").toInt

        val numUsers = cmd.getOptionValue("numusers").toInt
        val stopPoint = cmd.getOptionValue("stoppoint").toInt

        val isBefore = cmd.getOptionValue("mode").equals("before")

        println("starting up... numThreads: " + numThreads + " numUsers: " + numUsers)
        logger.debug("Starting up Client Process...")
        val cp = new ClientProcess(host, port, clientId, numThreads)
        println("initializing...")
        cp.initialize(createSizeLoadFunc)
        println("starting!")
        cp.start
        println("main thread done!")
    }

    def createSizeLoadFunc:(Int,Int,Environment)=>Unit = {
        val fn = (clientId:Int,threadId:Int,env:Environment) => {
            val dbFile = new File("target/db9000/00000000.jdb") 
            if ( !dbFile.isFile ) throw new FileNotFoundException("No db file found")
            implicit val implicitEnv = env 
            var counter = 0
            while ( dbFile.length < 1000000L ) {
                // add chunks of users
                println("dbfile length is: " + dbFile.length)
                println("adding users...")
                (counter to (counter+10000)).foreach((i)=> {
                    val user = new user
                    val str = "client"+clientId+"_thread"+threadId+"_user"+i
                    user.name(str)
                    user.email(str+"@test.com")
                    user.save
                })
            }
        }
        fn
    }

    def createLoadFunc(numUsers:Int,stopPoint:Int,isBefore:Boolean):(Int,Int,Environment)=>Unit = {
        val fn = (clientId:Int,threadId:Int,env:Environment) => {
            val cutOff = Math.floor((stopPoint/100.0)*numUsers).toInt
            val range = isBefore match {
                case true  => (0 to cutOff)
                case false => (cutOff to numUsers)
            }
            implicit val implicitEnv = env 
            range.foreach((i) => {
                val user = new user
                println("creating user from thread:" + threadId + ", user:" + i)
                user.name("client"+clientId+"_thread"+threadId+"_user"+i)
                user.save
            })
            println("thread:" + threadId + " is done!")
        }
        fn
    }
   

}


