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
				val db = env.openDatabase(null, name, dbConfig)
				dbCache.put(name,db)
				db
			}
		}
	}
}
