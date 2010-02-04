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
	case class Namespace(db: Database, keySchema: Schema, comp: AvroComparator, policy: RangePolicy)
	var namespaces: Map[String, Namespace] = new scala.collection.immutable.HashMap[String, Namespace]

	val outstandingRequests = new ArrayBlockingQueue[Runnable](1024)
	val executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS, outstandingRequests)

	implicit def mkDbe(buff: ByteBuffer): DatabaseEntry = new DatabaseEntry(buff.array, buff.position, buff.remaining)
	implicit def mkByteBuffer(dbe: DatabaseEntry) = ByteBuffer.wrap(dbe.getData)

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
			case tsr: TestSetRequest => {
				val ns = namespaces(tsr.namespace)
				val txn = env.beginTransaction(null, null)
				val dbeKey: DatabaseEntry = tsr.key
				val dbeEv = new DatabaseEntry()

				/* Set up partial get if the existing value specifies a prefix length */
				if(tsr.prefixMatch) {
					dbeEv.setPartial(true)
					dbeEv.setPartialLength(tsr.expectedValue.position)
				}

				/* Get the current value */
				ns.db.get(txn, dbeKey, dbeEv, LockMode.READ_COMMITTED)

				/* Throw exception if expected value doesnt match present value */
				if((dbeEv.getData == null && tsr.expectedValue != null) ||
					 (dbeEv.getData != null && tsr.expectedValue == null) ||
					 (dbeEv.getData != null && tsr.expectedValue != null && !dbeEv.equals(tsr.expectedValue))) {
					txn.abort
					val tsf = new TestAndSetFailure
					tsf.key = dbeKey
					tsf.currentValue = dbeEv
				}

				/* Otherwise perform the put and commit */
				if(tsr.value == null)
					ns.db.delete(txn, dbeKey)
				else {
					val dbeValue: DatabaseEntry = tsr.value
					ns.db.put(txn, dbeKey, dbeValue)
				}
				txn.commit
				reply(null)
			}
			case grr: GetRangeRequest => {
			}
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

	private def iterateOverRange(ns: Namespace, range: KeyRange, func: (Array[Byte], Array[Byte]) => Unit): Unit = {
		val dbeKey = new DatabaseEntry()
		val dbeValue = new DatabaseEntry()
		val cur = ns.db.openCursor(null, null)

		var status: OperationStatus =
			if(!range.backwards && range.minKey == null) {
				//Starting from neg inf and working our way forward
				cur.getFirst(dbeKey, dbeValue, null)
			}
			else if(!range.backwards) {
				//Starting d minKey and working our way forward
				cur.getSearchKeyRange(range.minKey, dbeValue, null)
			}
			else if(range.maxKey == null) {
				//Starting from inf and working our way backwards
				cur.getLast(dbeKey, dbeValue, null)
			}
			else { //Starting from maxKey and working our way back
				// Check if maxKey is past the last key in the database, if so start from the end
				if(cur.getSearchKeyRange(range.maxKey, dbeValue, null) == OperationStatus.NOTFOUND)
					cur.getLast(dbeKey, dbeValue, null)
				else
					OperationStatus.SUCCESS
			}

		var toSkip: Int = if(range.offset == null) -1 else range.offset.intValue()
		var remaining: Int = if(range.limit == null) -1 else range.limit.intValue()

		if(!range.backwards) {
			while(toSkip > 0 && status == OperationStatus.SUCCESS) {
				status = cur.getNext(dbeKey, dbeValue, null)
				toSkip -= 1
			}

			val stopKey = new Array[Byte](range.maxKey.limit)
			range.maxKey.duplicate.get(stopKey)


			status = cur.getCurrent(dbeKey, dbeValue, null)
			while(status == OperationStatus.SUCCESS &&
						remaining != 0 &&
						ns.comp.compare(dbeKey.getData, stopKey) > 0) {
				func(dbeKey.getData, dbeValue.getData)
				status = cur.getNext(dbeKey, dbeValue, null)
				remaining -= 1
			}
		}
		else {
			while(toSkip > 0 && status == OperationStatus.SUCCESS) {
				status = cur.getPrev(dbeKey, dbeValue, null)
				toSkip -= 1
			}

			val stopKey = new Array[Byte](range.minKey.limit)
			range.minKey.duplicate.get(stopKey)

			status = cur.getCurrent(dbeKey, dbeValue, null)
			while(status == OperationStatus.SUCCESS &&
						remaining != 0 &&
						ns.comp.compare(dbeKey.getData, stopKey) < 0) {
				func(dbeKey.getData, dbeValue.getData)
				status = cur.getPrev(dbeKey, dbeValue, null)
				remaining -= 1
			}
		}

		cur.close
	}

	def openNamespace(ns: String, partition: String): Unit = {
		namespaces.synchronized {
			val nsRoot = root.get("namespaces").get(ns)
			val keySchema = new String(nsRoot("keySchema").data)
			//val policy = partition("policy").data
			//val partition = nsRoot("partitions").get(partition)

			val comp = new AvroComparator(keySchema)
			val dbConfig = new DatabaseConfig
			dbConfig.setAllowCreate(true)
			dbConfig.setBtreeComparator(comp)
			dbConfig.setTransactional(true)

			namespaces += ((ns, Namespace(env.openDatabase(null, ns, dbConfig), Schema.parse(keySchema), comp, null)))
		}
	}

	def receiveMessage(src: RemoteNode, msg:StorageRequest): Unit = {
		executor.execute(new Request(src, msg))
	}
}
