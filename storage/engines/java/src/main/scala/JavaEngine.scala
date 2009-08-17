package edu.berkeley.cs.scads.storage

import java.io.File

import org.apache.commons.cli.Options
import org.apache.commons.cli.GnuParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.HelpFormatter
import org.apache.log4j.Logger

import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig

import edu.berkeley.cs.scads.thrift.StorageEngine

import org.apache.thrift.server.THsHaServer
import org.apache.thrift.transport.TNonblockingServerSocket
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

object JavaEngine {
	def main(args: Array[String]) = {
		val logger = Logger.getLogger("scads.engine")

		val options = new Options();
		options.addOption("p", "port",  true, "the port to run the thrift server on");
		options.addOption("d", "dbdir",  true, "directory to to store the database environment in");
		options.addOption("h", "help",  false, "print usage information");

		val parser = new GnuParser();
		val cmd = parser.parse( options, args);

		if(cmd.hasOption("help")) {
			val formatter = new HelpFormatter()
			formatter.printHelp("JavaEngine", options)
			System.exit(1)
		}

		val dbDir = cmd.hasOption("dbdir") match {
			case true => new File(cmd.getOptionValue("dbdir"))
			case false => new File("db")
		}
		logger.info("DbDir: " + dbDir)

		if(!dbDir.exists()) {
			dbDir.mkdir
		}

		val port = cmd.hasOption("port") match {
			case true => cmd.getOptionValue("port").toInt
			case false => 9000
		}
		logger.info("Port: " + port)

		logger.info("Opening the bdb environment")
		val config = new EnvironmentConfig();
		config.setAllowCreate(true);
		val env = new Environment(dbDir, config)
		logger.info("Environment opened")

		val processor = new StorageEngine.Processor(new StorageProcessor(env))
		val transport = new TNonblockingServerSocket(port)
		val protFactory = new TBinaryProtocol.Factory(true, true)
		val serverOpt = new THsHaServer.Options
		serverOpt.maxWorkerThreads=20
		serverOpt.minWorkerThreads=2
		val server = new THsHaServer(processor, transport, protFactory, serverOpt)

		logger.info("Starting server")
    	server.serve()
	}
}
