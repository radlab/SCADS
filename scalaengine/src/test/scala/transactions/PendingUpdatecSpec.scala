package edu.berkeley.cs.scads.test.transactions
import org.scalatest.WordSpec

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, WordSpec, OneInstancePerTest}
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher, ShouldMatchers}

import java.io.File
import java.lang.{ Integer => JInteger }
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

import org.apache.avro._
import specific._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._

import com.sleepycat.je.{Cursor,Database, DatabaseConfig, DatabaseException, DatabaseEntry, Environment, EnvironmentConfig, LockMode, OperationStatus, Durability, Transaction}

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.storage.transactions._
import edu.berkeley.cs.scads.storage.transactions.conflict._

case class KeyRec(var i: Int, var s: String) extends AvroRecord
case class ValueRec(var s: String, var i: Int) extends AvroRecord

abstract class FactoryType
case class BDBFactoryType() extends FactoryType
case class MapFactoryType() extends FactoryType

@RunWith(classOf[JUnitRunner])
class PendingUpdatesSpec extends WordSpec
with ShouldMatchers
with BeforeAndAfterAll
with BeforeAndAfterEach {
  private val keyBuilder = new KeyBuilder[KeyRec]
  private val valueBuilder = new ValueBuilder[ValueRec]

  private def makeScadsTempDir(name: String) = {
    val tempDir = File.createTempFile(name + "-scads.test", "testdb")
    /* This strange sequence of delete and mkdir is required so that BDB can
     * acquire a lock file in the created temp directory */
    tempDir.delete()
    tempDir.mkdir()
    tempDir
  }

  private def makeBDB(name: String) = {
    val config = new EnvironmentConfig
    config.setConfigParam(EnvironmentConfig.LOG_MEM_ONLY, "true")
    config.setAllowCreate(true)
    config.setTransactional(true)
    config.setSharedCache(true)
    config.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL,
                          JInteger.MAX_VALUE.toString)
    val dir = makeScadsTempDir(name)
    val env = new Environment(dir, config)
    new BDBTxDBFactory(new Environment(dir, config))
  }

  private def openFactory(dbFactory: FactoryType, factory: FactoryType,
                          name: String) = {
    lazy val bdbFactory = makeBDB(name)
    lazy val mapFactory = new MapTxDBFactory
    (dbFactory, factory) match {
      case (BDBFactoryType(), BDBFactoryType()) => (bdbFactory, bdbFactory)
      case (BDBFactoryType(), MapFactoryType()) => (bdbFactory, mapFactory)
      case (MapFactoryType(), BDBFactoryType()) => (mapFactory, bdbFactory)
      case (MapFactoryType(), MapFactoryType()) => (mapFactory, mapFactory)
    }
  }

  // returns Version update lists
  private def insertVersionUpdates(numKeys: Int) = {
    0 until numKeys map (i => {
      val k = KeyRec(i, i.toString)
      val m = TxRecordMetadata(0, List())
      val v = ValueRec(i.toString, i)
      VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
    })
  }
  // Returns single Version update list
  private def singleVersionUpdate(key: Int, value: Int, version: Long) = {
    val k = KeyRec(key, key.toString)
    val m = TxRecordMetadata(version, List())
    val v = ValueRec(value.toString, value)
    List(VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v)))
  }
  // returns Value update lists
  private def insertValueUpdates(numKeys: Int) = {
    0 until numKeys map (i => {
      val k = KeyRec(i, i.toString)
      val m = TxRecordMetadata(0, List())
      val v = ValueRec(i.toString, i)
      ValueUpdate(keyBuilder.toBytes(k), None, valueBuilder.toBytes(m, v))
    })
  }
  // Returns single Value update list
  private def singleValueUpdate(key: Int, value: Int, oldValue: Option[Int]) = {
    val k = KeyRec(key, key.toString)
    // The version is not compared with ValueUpdate
    val m = TxRecordMetadata(0, List())
    val v = ValueRec(value.toString, value)
    val oldV = oldValue.map(i => ValueRec(i.toString, i))
    List(ValueUpdate(keyBuilder.toBytes(k),
                     oldV.map(valueBuilder.toBytes(m, _)),
                     valueBuilder.toBytes(m, v)))
  }

  // DB states
  private def emptyDB(dbFactory: FactoryType, factory: FactoryType,
                      name: String) = {
    val (dbF, f) = openFactory(dbFactory, factory, name)
    val db = dbF.getNewDB[Array[Byte], Array[Byte]](name)
    (db, new PendingUpdatesController(db, f))
  }
  private def withVersionKeysDB(dbFactory: FactoryType, factory: FactoryType,
                                name: String, numKeys: Int) = {
    val (dbF, f) = openFactory(dbFactory, factory, name)
    val db = dbF.getNewDB[Array[Byte], Array[Byte]](name)
    val p = new PendingUpdatesController(db, f)

    val updates = insertVersionUpdates(numKeys)
    p.accept(ScadsXid(1, 1), updates) should be (true)
    p.commit(ScadsXid(1, 1), updates) should be (true)
    (db, p)
  }
  private def withValueKeysDB(dbFactory: FactoryType, factory: FactoryType,
                              name: String, numKeys: Int) = {
    val (dbF, f) = openFactory(dbFactory, factory, name)
    val db = dbF.getNewDB[Array[Byte], Array[Byte]](name)
    val p = new PendingUpdatesController(db, f)

    val updates = insertValueUpdates(numKeys)
    p.accept(ScadsXid(1, 1), updates) should be (true)
    p.commit(ScadsXid(1, 1), updates) should be (true)
    (db, p)
  }

  // Behavior of the pending update controller.
  def pendingUpdates(dbFactory: FactoryType, factory: FactoryType,
                     name: String) {

    "do nothing with no updates" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      db.get(null, keyBuilder.toBytes(KeyRec(1, "1"))) should be (None)
    }

    /* *********************************************************************
     * *************************** Reading Values **************************
     * ********************************************************************* */

    "read committed inserts (VERSION)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)
      0 until numKeys foreach (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(0, List())
        val v = ValueRec(i.toString, i)
        val b = db.get(null, keyBuilder.toBytes(k))
        b should not be (None)
        valueBuilder.fromBytes(b.get) should be ((m, Some(v)))
      })
    }

    "read committed inserts (VALUE)" in {
      val numKeys = 10
      val (db, p) = withValueKeysDB(dbFactory, factory, name, numKeys)
      0 until numKeys foreach (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(0, List())
        val v = ValueRec(i.toString, i)
        val b = db.get(null, keyBuilder.toBytes(k))
        b should not be (None)
        valueBuilder.fromBytes(b.get) should be ((m, Some(v)))
      })
    }

    "read committed updates (VERSION)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      val update = singleVersionUpdate(0, 1, 1)
      p.accept(ScadsXid(2, 2), update) should be (true)
      p.commit(ScadsXid(2, 2), update) should be (true)

      val k = KeyRec(0, "0")
      val m = TxRecordMetadata(1, List())
      val v = ValueRec("1", 1)
      val b = db.get(null, keyBuilder.toBytes(k))
      b should not be (None)
      valueBuilder.fromBytes(b.get) should be ((m, Some(v)))

      val update2 = singleVersionUpdate(0, 2, 2)
      p.accept(ScadsXid(3, 3), update2) should be (true)
      p.commit(ScadsXid(3, 3), update2) should be (true)

      val m2 = TxRecordMetadata(2, List())
      val v2 = ValueRec("2", 2)
      val b2 = db.get(null, keyBuilder.toBytes(k))
      b2 should not be (None)
      valueBuilder.fromBytes(b2.get) should be ((m2, Some(v2)))
    }

    "read committed updates (VALUE)" in {
      val numKeys = 10
      val (db, p) = withValueKeysDB(dbFactory, factory, name, numKeys)

      val update = singleValueUpdate(0, 1, 0)
      p.accept(ScadsXid(2, 2), update) should be (true)
      p.commit(ScadsXid(2, 2), update) should be (true)

      val k = KeyRec(0, "0")
      val v = ValueRec("1", 1)
      val b = db.get(null, keyBuilder.toBytes(k))
      b should not be (None)
      // Not comparing metadata.
      valueBuilder.fromBytes(b.get)._2 should be (Some(v))

      val update2 = singleValueUpdate(0, 2, 1)
      p.accept(ScadsXid(3, 3), update2) should be (true)
      p.commit(ScadsXid(3, 3), update2) should be (true)

      val v2 = ValueRec("2", 2)
      val b2 = db.get(null, keyBuilder.toBytes(k))
      b2 should not be (None)
      // Not comparing metadata.
      valueBuilder.fromBytes(b2.get)._2 should be (Some(v2))
    }

    "not read uncommitted (but accepted) inserts (VERSION)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val numKeys = 10
      val updates = insertVersionUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      0 until numKeys foreach (i => {
        db.get(null, keyBuilder.toBytes(KeyRec(i, i.toString))) should be (None)
      })
    }

    "not read uncommitted (but accepted) inserts (VALUE)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val numKeys = 10
      val updates = insertValueUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      0 until numKeys foreach (i => {
        db.get(null, keyBuilder.toBytes(KeyRec(i, i.toString))) should be (None)
      })
    }

    "not read accepted then aborted inserts (VERSION)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.abort(ScadsXid(1, 1))
      0 until numKeys foreach (i => {
        db.get(null, keyBuilder.toBytes(KeyRec(i, i.toString))) should be (None)
      })
    }

    "not read accepted then aborted inserts (VALUE)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.abort(ScadsXid(1, 1))
      0 until numKeys foreach (i => {
        db.get(null, keyBuilder.toBytes(KeyRec(i, i.toString))) should be (None)
      })
    }

    /* *********************************************************************
     * ************************** Accepting Values *************************
     * ********************************************************************* */

    "accept new inserts for non-existing keys (VERSION)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val numKeys = 10
      val updates = insertVersionUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
    }

    "accept new inserts for non-existing keys (VALUE)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val numKeys = 10
      val updates = insertValueUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)
    }

    "accept new inserts for non-conflicting keys (VERSION)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // No conflict with pending keys
      val updatesMixed = singleVersionUpdate(numKeys, numKeys, 0)
      p.accept(ScadsXid(4, 4), updatesMixed) should be (true)
    }

    "accept new inserts for non-conflicting keys (VALUE)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // No conflict with pending keys
      val updatesMixed = singleValueUpdate(numKeys, numKeys, None)
      p.accept(ScadsXid(4, 4), updatesMixed) should be (true)
    }

    "accept new updates with the correct version (VERSION)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      // Correct version (should be 1)
      val update = singleVersionUpdate(0, 0, 1 /* version */)
      p.accept(ScadsXid(2, 2), update) should be (true)
    }

    "accept new updates with the correct value (VALUE)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      // Correct old value is 0
      val update = singleValueUpdate(0, 1, 0)
      p.accept(ScadsXid(2, 2), update) should be (true)
    }

    "accept new updates with the correct value and wrong version (VALUE)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      // Correct old value is 0
      val update = singleValueUpdate(0, 1, 0)
      p.accept(ScadsXid(2, 2), update) should be (true)
      p.commit(ScadsXid(2, 2), update) should be (true)

      // Correct old value is 1
      val update2 = singleValueUpdate(0, 2, 1)
      p.accept(ScadsXid(3, 3), update2) should be (true)
    }

    /* *********************************************************************
     * ************************* Conflict Detection ************************
     * ********************************************************************* */

    "detect conflicts for pending updates (VERSION)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // Conflicts with existing pending update, record 0
      val updatesFirst = singleVersionUpdate(0, 0, 0)
      p.accept(ScadsXid(2, 2), updatesFirst) should be (false)

      // Conflicts with existing pending update, record numKeys - 1
      val updatesLast = singleVersionUpdate(numKeys - 1, numKeys - 1, 0)
      p.accept(ScadsXid(3, 3), updatesLast) should be (false)

      // Conflicts with existing pending update, record 0
      val updatesMixed = singleVersionUpdate(numKeys, numKeys, 0) :::
        singleVersionUpdate(0, 0, 0)
      p.accept(ScadsXid(4, 4), updatesMixed) should be (false)

      // Conflicts with existing pending update, all records
      p.accept(ScadsXid(5, 5), updates) should be (false)
    }

    "detect conflicts for pending updates (VALUE)" in {
      val numKeys = 10
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(numKeys)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // Conflicts with existing pending update, record 0
      val updatesFirst = singleValueUpdate(0, 0, None)
      p.accept(ScadsXid(2, 2), updatesFirst) should be (false)

      // Conflicts with existing pending update, record numKeys - 1
      val updatesLast = singleValueUpdate(numKeys - 1, numKeys - 1, None)
      p.accept(ScadsXid(3, 3), updatesLast) should be (false)

      // Conflicts with existing pending update, record 0
      val updatesMixed = singleValueUpdate(numKeys, numKeys, None) :::
        singleValueUpdate(0, 0, None)
      p.accept(ScadsXid(4, 4), updatesMixed) should be (false)

      // Conflicts with existing pending update, all records
      p.accept(ScadsXid(5, 5), updates) should be (false)
    }

    "detect conflicts for committed updates (VERSION)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      // Wrong version (should be 1)
      val update = singleVersionUpdate(0, 0, 0 /* version */)
      p.accept(ScadsXid(2, 2), update) should be (false)
    }

    "detect conflicts for committed updates (VALUE)" in {
      val numKeys = 10
      val (db, p) = withValueKeysDB(dbFactory, factory, name, numKeys)

      // Wrong old value (should be 0)
      val update = singleValueUpdate(0, 0, 100)
      p.accept(ScadsXid(2, 2), update) should be (false)
    }

    "detect conflicts for mixed physical pending updates (VERSION + VALUE)" in {
      val numKeys = 10
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, numKeys)

      val update1 = singleVersionUpdate(0, 1, 1)
      p.accept(ScadsXid(2, 2), update1) should be (true)

      // Conflict, because of the same key.
      val update2 = singleValueUpdate(0, 2, 0)
      p.accept(ScadsXid(3, 3), update2) should be (false)

      // commit the first tx.
      p.commit(ScadsXid(2, 2), update1) should be (true)

      // This time this should succeed.
      val update3 = singleValueUpdate(0, 2, 1)
      p.accept(ScadsXid(3, 3), update3) should be (true)

      // Conflict, because of the same key
      val update4 = singleVersionUpdate(0, 3, 2)
      p.accept(ScadsXid(4, 4), update4) should be (false)
    }

    /* *********************************************************************
     * ************************** getDecision ******************************
     * ********************************************************************* */

    "return commit for committed updates (VERSION)" in {
      val (db, p) = withVersionKeysDB(dbFactory, factory, name, 10)
      p.getDecision(ScadsXid(1, 1)) should be (Status.Commit)
    }

    "return commit for committed updates (VALUE)" in {
      val (db, p) = withValueKeysDB(dbFactory, factory, name, 10)
      p.getDecision(ScadsXid(1, 1)) should be (Status.Commit)
    }

    "return accept for accepted updates (VERSION)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.getDecision(ScadsXid(1, 1)) should be (Status.Accept)
    }

    "return accept for accepted updates (VALUE)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.getDecision(ScadsXid(1, 1)) should be (Status.Accept)
    }

    "return reject for rejected updates (VERSION)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // Conflicts with existing pending update, record 0
      val updatesFirst = singleVersionUpdate(0, 0, 0)
      p.accept(ScadsXid(2, 2), updatesFirst) should be (false)
      p.getDecision(ScadsXid(2, 2)) should be (Status.Reject)
    }

    "return reject for rejected updates (VALUE)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)

      // Conflicts with existing pending update, record 0
      val updatesFirst = singleValueUpdate(0, 0, None)
      p.accept(ScadsXid(2, 2), updatesFirst) should be (false)
      p.getDecision(ScadsXid(2, 2)) should be (Status.Reject)
    }

    "return abort for aborted updates (VERSION)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertVersionUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.abort(ScadsXid(1, 1))
      p.getDecision(ScadsXid(1, 1)) should be (Status.Abort)
    }

    "return abort for aborted updates (VALUE)" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      val updates = insertValueUpdates(10)
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.abort(ScadsXid(1, 1))
      p.getDecision(ScadsXid(1, 1)) should be (Status.Abort)
    }

    "return unknown for unknown updates" in {
      val (db, p) = emptyDB(dbFactory, factory, name)
      p.getDecision(ScadsXid(1, 1)) should be (Status.Unknown)
    }

  }

  "A PendingUpdateController" when {
    "using BDB with a BDB factory" should {
      behave like pendingUpdates(BDBFactoryType(), BDBFactoryType(), "A")
    }
    "using BDB with a hashmap factory" should {
      behave like pendingUpdates(BDBFactoryType(), MapFactoryType(), "B")
    }
    "using a hashmap with a BDB factory" should {
      behave like pendingUpdates(MapFactoryType(), BDBFactoryType(), "C")
    }
    "using a hashmap with a hashmap factory" should {
      behave like pendingUpdates(MapFactoryType(), MapFactoryType(), "D")
    }
  }
}
