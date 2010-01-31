package edu.berkeley.cs.scads.storage

import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus, Durability}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

class StorageHandler(env: Environment) extends AvroChannelManager[StorageResponse, StorageRequest] {
	case class Namespace(db: Database, policy: RangePolicy)
	var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

	val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
	val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

	class Request(src: RemoteNode, req: StorageRequest) extends Runnable {
		def reply(body: AnyRef) = {
			val resp = new StorageResponse
			resp.body = body
			resp.dest = req.src	
			sendMessage(src, resp)
		}

		val process: PartialFunction[Object, Unit] = {
			case gr: GetRequest =>
//TODO			case pr: PutRequest =>
//TODO			case grr: GetRangeRequest =>
//TODO			case cr: ConfigureRequest =>
		}

		def run():Unit = {
			try {
				process(req.body)
			}
			catch {
				case e: Throwable => {
					val resp = new ProcessingException
					resp.cause = e.toString()
					resp.stacktrace = e.getStackTrace().mkString("\n")
					reply(resp)
				}
			}
		}
	}

	def receiveMessage(src: RemoteNode, msg:StorageRequest): Unit = {
		executor.execute(new Request(src, msg))
	}
}
