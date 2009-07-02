package edu.berkeley.cs.scads.test

import org.scalatest.Suite
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.nodes.StorageNode
import edu.berkeley.cs.scads.nodes.TestableBdbStorageNode
import edu.berkeley.cs.scads.nodes.TestableSimpleStorageNode

import edu.berkeley.cs.scads.thrift.KeyStore
import edu.berkeley.cs.scads.thrift.Record

abstract class KeyStoreSuite extends Suite { 
  val connection: KeyStore.Client

  def testSimpleGetPut() = {
    def rec = new Record("test", "test")
    connection.put("simple", rec)
    assert(connection.get("simple", "test") === rec)
  }
}

class BdbKeyStoreTest extends KeyStoreSuite {
  val n = new TestableBdbStorageNode()
  val connection = n.createConnection()
}

class SimpleKeyStoreTest extends KeyStoreSuite {
  val n = new TestableSimpleStorageNode()
  val connection = n.createConnection()
}