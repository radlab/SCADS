package edu.berkeley.cs.scads.storage

import scala.collection.jcl.Conversions

import edu.berkeley.cs.scads.thrift._

import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus, Durability}

class StorageProcessor(env: Environment, deferedWrites: Boolean) extends StorageEngine.Iface {
	class CachedDb(val handle: Database, val respPolicy: RangedPolicy)

	val logger = Logger.getLogger("scads.storageprocessor")
	val dbCache = new scala.collection.mutable.HashMap[String, CachedDb]
	val respPolicyDb = openDatabase("resp_policies")

	if(deferedWrites)
		logger.info("StorageProcessor created with deferedWrites")

	def count_set(ns: String, rs: RecordSet): Int = {
        var count = 0
        iterateOverSet(ns, rs, (rec) => { count += 1 })
        count
    }

	def get_set(ns: String, rs: RecordSet): java.util.List[Record] = {
		val buffer = new java.util.ArrayList[Record]
		iterateOverSet(ns, rs, (rec) => {
			buffer.add(rec)
		})
		buffer
	}

	def get(ns: String, key: String): Record = {
		val db = getDatabase(ns)
		val dbeKey = new DatabaseEntry(key.getBytes)
		val dbeValue = new DatabaseEntry()

		if(!db.respPolicy.contains(key))
			throw new NotResponsible

		db.handle.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)

		if(dbeValue.getData == null)
			new Record(key, null)
		else
			new Record(key, new String(dbeValue.getData))
	}

	def async_put(ns: String, rec: Record): Unit = {
   	val db = getDatabase(ns)
		val key = new DatabaseEntry(rec.key.getBytes)
    val txn = env.beginTransaction(null, null)

		if(rec.value == null)
			db.handle.delete(txn, key)
		else
			db.handle.put(txn, key, new DatabaseEntry(rec.value.getBytes))

    txn.commit(Durability.COMMIT_NO_SYNC)
  }

	def put(ns: String, rec: Record): Boolean = {
		val db = getDatabase(ns)
		val key = new DatabaseEntry(rec.key.getBytes)
		val txn = env.beginTransaction(null, null)

		if(rec.value == null)
			db.handle.delete(txn, key)
		else
			db.handle.put(txn, key, new DatabaseEntry(rec.value.getBytes))

		if(deferedWrites)
			txn.commit(Durability.COMMIT_NO_SYNC)
		else
			txn.commit(Durability.COMMIT_SYNC)

		true
	}

	def test_and_set(ns: String, rec: Record, existingValue: ExistingValue): Boolean = {
		val db = getDatabase(ns)
		val txn = env.beginTransaction(null, null)
		val dbeKey = new DatabaseEntry(rec.key.getBytes)
		val dbeEv = new DatabaseEntry()

		/* Set up partial get if the existing value specifies a prefix length */
		if(existingValue.isSetPrefix()) {
			dbeEv.setPartial(true)
			dbeEv.setPartialLength(existingValue.getPrefix)
		}

		/* Get the current value */
		db.handle.get(txn, dbeKey, dbeEv, LockMode.READ_COMMITTED)

		/* Throw exception if expected value doesnt match present value */
		if((dbeEv.getData == null && existingValue.getValue != null) ||
		   (dbeEv.getData != null && existingValue.getValue == null) ||
		   (dbeEv.getData != null && existingValue.getValue != null && dbeEv.getData.compare(existingValue.getValue.getBytes) != 0)) {
			txn.abort

			throw new TestAndSetFailure(createString(dbeEv.getData))
		}

		/* Otherwise perform the put and commit */
		val dbeValue = new DatabaseEntry(rec.value.getBytes)
		if(rec.value == null)
			db.handle.delete(txn, dbeKey)
		else
			db.handle.put(txn, dbeKey, dbeValue)
		txn.commit
		true
	}

	def copy_set(ns: String, rs: RecordSet, h: String): Boolean = {
        val records = get_set(ns,rs)
        if ( records.size == 0 ) {
            true
        } else {
            val hostname = h.split(":")(0)
            val port = h.split(":")(1).toInt
            val sn = new StorageNode(hostname, port)
            sn.useConnection( (c) => {
                val iter = records.iterator
                while (iter.hasNext) {
                    c.put(ns, iter.next)
                }
            })
            true
        }
	}

	def get_responsibility_policy(ns: String): java.util.List[RecordSet] = {
		val ret = new java.util.LinkedList[RecordSet]
		getDatabase(ns).respPolicy.policy.foreach(p => {
			val recSet = new RecordSet
			val rangeSet = new RangeSet
			if(p._1 != null) rangeSet.setStart_key(p._1)
			if(p._2 != null) rangeSet.setEnd_key(p._2)
			recSet.setType(RecordSetType.RST_RANGE)
			recSet.setRange(rangeSet)
			ret.add(recSet)
		})
		ret
	}

	def remove_set(ns: String, rs: RecordSet): Boolean = {
        val records = get_set(ns,rs)
        if ( records.size == 0 ) {
            true
        } else {
            val db = getDatabase(ns)
            val txn = env.beginTransaction(null, null)
            val iter = records.iterator
            while (iter.hasNext) {
                db.handle.delete(txn, new DatabaseEntry(iter.next.key.getBytes))
            }
            txn.commit
            true
        }
	}

	def set_responsibility_policy(ns : String, policy: java.util.List[RecordSet]): Boolean = {
		val rangedPolicy = new RangedPolicy(RangedPolicy.convert(policy).toArray)
		val bytes = rangedPolicy.getBytes

		respPolicyDb.put(null, new DatabaseEntry(ns.getBytes), new DatabaseEntry(bytes))
		dbCache.put(ns, new CachedDb(getDatabase(ns).handle, rangedPolicy))
		true
	}

	def sync_set(ns: String, rs: RecordSet, h: String, policy: ConflictPolicy): Boolean = {
		true
	}

	private def openDatabase(name: String): Database = {
		val dbConfig = new DatabaseConfig()
		dbConfig.setAllowCreate(true)
		dbConfig.setTransactional(true)

		if(System.getProperty("deferred.write") != null)
			dbConfig.setDeferredWrite(true)

		logger.info("Opening database " + name + ", config: " + dbConfig)

		env.openDatabase(null, name, dbConfig)
	}

	protected def getDatabase(name: String): CachedDb = {
		dbCache.get(name) match {
			case Some(db) => db
			case None => {
				val db = openDatabase(name)

				val dbeDbName = new DatabaseEntry(name.getBytes())
				val dbeRespPolicy = new DatabaseEntry()

				val policy = if(respPolicyDb.get(null, dbeDbName, dbeRespPolicy, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
					RangedPolicy(dbeRespPolicy.getData())
				}
				else {
					new RangedPolicy(Array[(String, String)]((null, null)))
				}

				val cacheEntry = new CachedDb(db, policy)
				dbCache.put(name,cacheEntry)
				cacheEntry
			}
		}
	}

	/* Pass all records in namespace ns that fall in RangeSet rs to the provided function */
	private def iterateOverSet(ns: String, rs: RecordSet, func: (Record) => Unit): Unit = {
		val range = rs.getRange
		val dbeKey = new DatabaseEntry()
		val dbeValue = new DatabaseEntry()
		val cur = getDatabase(ns).handle.openCursor(null, null)
		val total = if(rs.getRange.isSetOffset) rs.getRange.getOffset + rs.getRange.getLimit else rs.getRange.getLimit
		var status: OperationStatus = null
		var count = 0

		val (forward, endValue) =
			if(range.getStart_key == null) {
				status = cur.getFirst(dbeKey, dbeValue, null)
				(true,if(range.getEnd_key == null) null else range.getEnd_key.getBytes)
			}
			else if(range.getEnd_key == null) {
				dbeKey.setData(range.getStart_key.getBytes)
				status = cur.getSearchKeyRange(dbeKey, dbeValue, null)
				(true, null)
			}
			else if(range.getStart_key.compare(range.getEnd_key) < 0) {
				dbeKey.setData(range.getStart_key.getBytes)
				status = cur.getSearchKeyRange(dbeKey, dbeValue, null)
				(true, range.getEnd_key.getBytes)
			}
			else {
				dbeKey.setData(range.getStart_key.getBytes)
				status = cur.getSearchKeyRange(dbeKey, dbeValue, null)
				if(status != OperationStatus.NOTFOUND && dbeKey.getData.compare(range.getStart_key.getBytes) != 0)
					status = cur.getPrev(dbeKey, dbeValue, null)
				if(status == OperationStatus.NOTFOUND)
					status = cur.getLast(dbeKey, dbeValue, null)
				(false, range.getEnd_key.getBytes)
			}

		while(status != OperationStatus.NOTFOUND &&
					(endValue == null || (forward && dbeKey.getData.compare(endValue) <= 0) || ((!forward) && dbeKey.getData.compare(endValue) >= 0)) &&
					(!rs.getRange.isSetLimit || count < total)) {
			if(!rs.getRange.isSetOffset || count >= rs.getRange.getOffset)
				func(new Record(createString(dbeKey.getData), createString(dbeValue.getData)))
			count += 1
			if(forward)
				status = cur.getNext(dbeKey, dbeValue, null)
			else
				status = cur.getPrev(dbeKey, dbeValue, null)
		}
		cur.close
	}

	private def createString(bytes: Array[Byte]): String =
		if(bytes == null)
			null
		else
			new String(bytes)
}
