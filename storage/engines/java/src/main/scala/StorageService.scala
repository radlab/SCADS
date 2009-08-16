package edu.berkeley.cs.scads.storage

import edu.berkeley.cs.scads.thrift._
import com.sleepycat.je.Environment
import com.sleepycat.je.Database

class StorageProcessor(env: Environment) extends StorageEngine.Iface {
	def count_set(ns: String, rs: RecordSet): Int = 0

	def get_set(ns: String, rs: RecordSet): java.util.List[Record] = {
		null
	}

	def get(ns: String, key: String): Record = {
		null
	}

	def put(ns: String, rec: Record): Boolean = {
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
		env.openDatabase(null, name, null)
	}
}
