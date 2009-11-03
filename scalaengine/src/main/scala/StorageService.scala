package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode, OperationStatus}

class StorageProcessor(env: Environment) extends StorageEngine.Iface {
	val logger = Logger.getLogger("scads.storageprocessor")
	val dbCache = new scala.collection.mutable.HashMap[String, Database]

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
		db.get(null, dbeKey, dbeValue, LockMode.READ_COMMITTED)

		if(dbeValue.getData == null)
			new Record(key, null)
		else
			new Record(key, new String(dbeValue.getData))
	}

	def put(ns: String, rec: Record): Boolean = {
		val db = getDatabase(ns)
		val key = new DatabaseEntry(rec.key.getBytes)

		if(rec.value == null)
			db.delete(null, key)
		else
			db.put(null, key, new DatabaseEntry(rec.value.getBytes))

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
		db.get(txn, dbeKey, dbeEv, LockMode.READ_COMMITTED)

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
			db.delete(txn, dbeKey)
		else
			db.put(txn, dbeKey, dbeValue)
		txn.commit
		true
	}

	def copy_set(ns: String, rs: RecordSet, h: String): Boolean = {
		true
	}

	def get_responsibility_policy(ns: String): RecordSet = {
		null
	}

	def remove_set(ns: String, rs: RecordSet): Boolean = {
		true
	}

	def set_responsibility_policy(ns : String, policy: RecordSet): Boolean = {
		true
	}

	def sync_set(ns: String, rs: RecordSet, h: String, policy: ConflictPolicy): Boolean = {
		true
	}

	private def getDatabase(name: String): Database = {
		dbCache.get(name) match {
			case Some(db) => db
			case None => {
				val dbConfig = new DatabaseConfig()
				dbConfig.setAllowCreate(true)
				dbConfig.setTransactional(true)
				val db = env.openDatabase(null, name, dbConfig)
				dbCache.put(name,db)
				db
			}
		}
	}

	/* Pass all records in namespace ns that fall in RangeSet rs to the provided function */
	private def iterateOverSet(ns: String, rs: RecordSet, func: (Record) => Unit): Unit = {
		val dbeKey = rs.getRange.getStart_key match {
            case null => new DatabaseEntry()     
            case _    => new DatabaseEntry(rs.getRange.getStart_key.getBytes)
        }		
        val endValue = rs.getRange.getEnd_key match {
            case null => null
            case _    => rs.getRange.getEnd_key.getBytes
        }
		val dbeValue = new DatabaseEntry()
		val cur = getDatabase(ns).openCursor(null, null)
		var status = rs.getRange.getStart_key match {
            case null => cur.getFirst(dbeKey, dbeValue, null)
            case _    => cur.getSearchKeyRange(dbeKey, dbeValue, null)
        }
		var count = 0
		val total = rs.getRange.isSetOffset match {
				case true => rs.getRange.getOffset + rs.getRange.getLimit
				case false => rs.getRange.getLimit
			}

		while(status != OperationStatus.NOTFOUND && (rs.getRange.getEnd_key == null || dbeKey.getData.compare(endValue) <= 0) && (!rs.getRange.isSetLimit || count < total)) {
			if(!rs.getRange.isSetOffset || count >= rs.getRange.getOffset)
				func(new Record(createString(dbeKey.getData), createString(dbeValue.getData)))
			count += 1
			status = cur.getNext(dbeKey, dbeValue, null)
		}
		cur.close
	}

	private def createString(bytes: Array[Byte]): String =
		if(bytes == null)
			null
		else
			new String(bytes)
}
