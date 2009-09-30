package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.thrift._
import org.apache.log4j.Logger
import com.sleepycat.je.{Database, DatabaseConfig, DatabaseEntry, Environment, LockMode}


class StorageProcessor(env: Environment) extends StorageEngine.Iface {
	val logger = Logger.getLogger("scads.storageprocessor")
	val dbCache = new scala.collection.mutable.HashMap[String, Database]

	def count_set(ns: String, rs: RecordSet): Int = 0

	def get_set(ns: String, rs: RecordSet): java.util.List[Record] = {
		new java.util.ArrayList[Record]()
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
}
