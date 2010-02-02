package edu.berkeley.cs.scads.storage

import java.util.Comparator
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus, Durability}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.Schema

@serializable
class AvroComparator(val json: String) extends Comparator[Array[Byte]] with java.io.Serializable {
	@transient
	val schema = Schema.parse(json)
	
	def compare(o1: Array[Byte], o2: Array[Byte]): Int = 
		org.apache.avro.io.BinaryData.compare(o1, 0, o2, 0, schema)

	override def equals(other: Any): Boolean = other match {
		case ac: AvroComparator => json equals ac.json
		case _ => false
	}
}


class StorageHandler(env: Environment, root: ZooKeeperProxy#ZooKeeperNode) extends AvroChannelManager[StorageResponse, StorageRequest] {
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
			case cr: ConfigureRequest => {
				openNamespace(cr.namespace, cr.partition)
				reply(null)	
			}
			case gr: GetRequest => {
				
			}
			case pr: PutRequest => true
//			case grr: GetRangeRequest => true 
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

	def openNamespace(ns: String, partition: String): Unit = {
		namespaces.synchronized {
			val nsRoot = root.get("namespaces").get(ns)
			val keySchema = new String(nsRoot("keySchema").data)
			//val policy = partition("policy").data
			//val partition = nsRoot("partitions").get(partition)

			val dbConfig = new DatabaseConfig
			dbConfig.setAllowCreate(true)
			dbConfig.setBtreeComparator(new AvroComparator(keySchema))
			dbConfig.setTransactional(true)
			
			namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), null)))
		}
	}

	def receiveMessage(src: RemoteNode, msg:StorageRequest): Unit = {
		executor.execute(new Request(src, msg))
	}
}
