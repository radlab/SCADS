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

  // DB initial states
  private def emptyDB(dbFactory: FactoryType, factory: FactoryType,
                      name: String) = {
    val (dbF, f) = openFactory(dbFactory, factory, name)
    val db = dbF.getNewDB[Array[Byte], Array[Byte]](name)
    (db, new PendingUpdatesController(db, f))
  }

  // Behavior of the pending update controller.
  def pendingUpdates(dbFactory: FactoryType, factory: FactoryType,
                     name: String) {
    "do nothing with no updates" in {
      val (db, p) = emptyDB(dbFactory, factory, name + "-A")
      db.get(null, keyBuilder.toBytes(KeyRec(1, "1"))) should be (None)
    }
    "accept new inserts for non-existing keys" in {
      val (db, p) = emptyDB(dbFactory, factory, name + "-B")
      val numKeys = 10
      val updates = 0 until numKeys map (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(i, List())
        val v = ValueRec(i.toString, i)
        VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
      })
      p.accept(ScadsXid(1, 1), updates) should be (true)
    }
    "not read uncommitted (but accepted) inserts" in {
      val (db, p) = emptyDB(dbFactory, factory, name + "-C")
      val numKeys = 10
      val updates = 0 until numKeys map (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(i, List())
        val v = ValueRec(i.toString, i)
        VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
      })
      p.accept(ScadsXid(1, 1), updates) should be (true)
      0 until numKeys foreach (i => {
        db.get(null, keyBuilder.toBytes(KeyRec(i, i.toString))) should be (None)
      })
    }    
    "read committed inserts" in {
      val (db, p) = emptyDB(dbFactory, factory, name + "-D")
      val numKeys = 10
      val updates = 0 until numKeys map (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(i, List())
        val v = ValueRec(i.toString, i)
        VersionUpdate(keyBuilder.toBytes(k), valueBuilder.toBytes(m, v))
      })
      p.accept(ScadsXid(1, 1), updates) should be (true)
      p.commit(ScadsXid(1, 1), updates) should be (true)
      0 until numKeys foreach (i => {
        val k = KeyRec(i, i.toString)
        val m = TxRecordMetadata(i, List())
        val v = ValueRec(i.toString, i)
        val b = db.get(null, keyBuilder.toBytes(k))
        b should not be (None)
        valueBuilder.fromBytes(b.get) should be ((m, Some(v)))
      })
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
