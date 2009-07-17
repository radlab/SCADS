package edu.berkeley.cs.scads.client

import edu.berkeley.cs.scads.thrift.{Record, RecordSet, KeyStore,ExistingValue}

abstract class ClientLibrary extends KeyStore.Iface {
	def get(namespace: String, key: String): Record
	def get_set(namespace: String, keys: RecordSet): java.util.List[Record]
	def put(namespace: String, rec:Record): Boolean
	def count_set(namespace: String ,keys: RecordSet): Int
	def test_and_set(namespace: String, rec: Record, existing: ExistingValue): Boolean
}