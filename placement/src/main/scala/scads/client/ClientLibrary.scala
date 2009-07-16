package edu.berkeley.cs.scads.client

import edu.berkeley.cs.scads.thrift.Record
import edu.berkeley.cs.scads.thrift.RecordSet

abstract class ClientLibrary {
	def get(namespace: String, key: String): Record
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]
	def put(namespace: String, rec:Record): Boolean
}