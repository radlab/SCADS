package edu.berkeley.cs.scads.storage

import java.util.Comparator
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.nio.ByteBuffer

import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus, Durability}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import org.apache.avro.Schema

@serializable
class AvroComparator(val json: String) extends Comparator[Array[Byte]] with java.io.Serializable {
	@transient
	lazy val schema = Schema.parse(json)

	def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
		println("schema: " + schema)
		org.apache.avro.io.BinaryData.compare(o1, 0, o2, 0, schema)
	}

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

	implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)

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
				val ns = namespaces(gr.namespace)
				val dbeKey: DatabaseEntry = gr.key
				val dbeValue = new DatabaseEntry

				println("getting key: " + gr.key)
				println("getting key: " + dbeKey)

				ns.db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)
				val retRec = new Record
				retRec.key = dbeKey.getData
				retRec.value = dbeValue.getData

				reply(retRec)
			}
			case pr: PutRequest => {
				val ns = namespaces(pr.namespace)
				val key: DatabaseEntry = pr.key
				val txn = env.beginTransaction(null, null)

				println("putting key: " + key)

				if(pr.value == null)
					ns.db.delete(txn, key)
				else
					ns.db.put(txn, key, pr.value)

				txn.commit(Durability.COMMIT_NO_SYNC)
				reply(null)
			}
//			case grr: GetRangeRequest => true
		}

		def run():Unit = {
			try {
				println(req)
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
