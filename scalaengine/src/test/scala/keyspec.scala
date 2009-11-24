package edu.berkeley.cs.scads.storage

import org.specs._
import org.specs.runner.JUnit4
import scala.collection.jcl.Conversions
import org.apache.log4j.BasicConfigurator

import edu.berkeley.cs.scads.thrift._
import java.io.File
import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig

abstract class KeyStoreSpec extends SpecificationWithJUnit("KeyStore Specification") {
	val ks: KeyStore.Iface

	"a keystore" should {
		"store key/value pairs such that" >> {
			"updates persist" in {
				val rec = new Record("key", "value")
				ks.put("persistTest", rec) must_== true
				ks.get("persistTest", "key") must_== rec
			}

			"updates change the value" in {
				val rec1 = new Record("key", "value1")
				val rec2 = new Record("key", "value2")

				ks.put("updateTest", rec1) must_== true
				ks.put("updateTest", rec2) must_== true
				ks.get("updateTest", "key") must_== rec2
			}

			"keys with null values are deleted" in {
				val rec = new Record("key", "value")
				val delRec = new Record("key", null)

				ks.put("nullVal", rec) must_== true
				ks.put("nullVal", delRec) must_== true
				ks.get("nullVal", "key") must_== delRec
			}

			"empty strings can be stored" in {
				val rec = new Record("key", "")
				ks.put("emptyString", rec) must_== true
				ks.get("emptyString", "key") must_== rec
			}
		}

		"provide test/set that" >> {

			"succedes when unchanged" in {
				val rec = new Record("tassuc","tassuc1")
				ks.put("tass",rec)
				val ev = new ExistingValue("tassuc1",0)
				ev.unsetPrefix()
				rec.value = "tassuc2"
				ks.test_and_set("tass",rec,ev) must_== true
				ks.get("tass","tassuc") must_== rec
			}

			"fails when the value has changed" in {
				val rec = new Record("tasfail","tasfail1")
				ks.put("tasf",rec)
				val ev = new ExistingValue("failval",0)
				ev.unsetPrefix()
				rec.value = "tasfail2"
				try {
					ks.test_and_set("tasf",rec,ev)
					fail("Exception not throw for changed test/set")
				} catch {
					case tsf: TestAndSetFailure => tsf.currentValue must_== "tasfail1"
				}
			}

			"succeds when null is expected" in {
				val ev = new ExistingValue()
				ev.unsetValue()
				ev.unsetPrefix()
				val rec = new Record("tasnull","tasnull")
				ks.test_and_set("tasn",rec,ev) must_== true
			}

			"fails when null isn't there" in {
				val ev = new ExistingValue()
				ev.unsetValue()
				ev.unsetPrefix()
				val rec = new Record("tasnullf","tasnullf")

				ks.put("nullfailure", rec)
				try {
					ks.test_and_set("nullfailure", rec, ev)
					fail("Exception not throw when test/set")
				}
				catch {
					case tsf: TestAndSetFailure => tsf.currentValue must_== "tasnullf"
				}
				0
			}

			"succeeds when a prefix is unchanged" in {
				val rec = new Record("tassucp","tassucp1ignore")
				ks.put("tassp",rec) must_== true
				val ev = new ExistingValue("tassucp",7)
				rec.value = "tassucp2"
				ks.put("tassp", rec) must_== true
				ks.test_and_set("tassp",rec,ev) must_== true
				ks.get("tassp","tassucp").value must_== "tassucp2"
			}

			"fails when a prefix has changes" in {
				val rec = new Record("tasfailp","tasfailp1ignore")
				ks.put("tasfp",rec) must_== true
				val ev = new ExistingValue("failval",7)

				try {
					ks.test_and_set("tasfp",rec,ev)
					fail("Didn't throw exception on changed prefix")
				} catch {
					case tsf: TestAndSetFailure => tsf.currentValue must_== "tasfail"
				}
				ks.get("tasfp","tasfailp").value must_== "tasfailp1ignore"
			}
		}


		val keyFormat = new java.text.DecimalFormat("0000000000000000")
			def recSet(start: Int, end:Int) = {
				val recSet = new RecordSet
				val rangeSet = new RangeSet
				if ( start != Math.MIN_INT ) rangeSet.setStart_key(keyFormat.format(start))
				if ( end != Math.MAX_INT ) rangeSet.setEnd_key(keyFormat.format(end))
				recSet.setType(RecordSetType.RST_RANGE)
				recSet.setRange(rangeSet)
				recSet
			}

		"have a remove_set function that" >> {
			val records = (3 to 7).toList.map((i) => new Record(keyFormat.format(i), i.toString))
			records.foreach(ks.put("set", _))

			"correctly removes entire sets" in {
				ks.asInstanceOf[StorageProcessor].remove_set("set", recSet(3,7)) must_== true
					ks.count_set("set", recSet(3,7)) must_== 0
			}

		}

		"have a count_set function that" >> {
			val records = (3 to 7).toList.map((i) => new Record(keyFormat.format(i), i.toString))
			records.foreach(ks.put("set", _))

			"correctly returns exact ranges counts" in {
                ks.count_set("set", recSet(3,7)) mustEqual records.size
			}

			"correctly returns in between range counts" in {
                ks.count_set("set", recSet(4,6)) mustEqual 3
			}

			"correctly return zero counts" in {
				ks.count_set("set", recSet(0,1)) mustEqual 0
			}

		}

		"do prefix matches correctly for" >> {
			def mkSet(start: String, end:String, limit: Int) = {
				val recSet = new RecordSet
				val rangeSet = new RangeSet
				if(start != null) rangeSet.setStart_key(start)
				if(end != null) rangeSet.setEnd_key(end)
				if(limit > 0) rangeSet.setLimit(limit)
				recSet.setType(RecordSetType.RST_RANGE)
				recSet.setRange(rangeSet)
				recSet
			}

			val records = (1 to 10).flatMap(i => {
				(1 to 10).map(j => {
					new Record("%02d%02d".format(i,j), i + "," + j)
				})
			})
			records.foreach(ks.put("prefix", _))

			"ascending prefixes" in {
				Conversions.convertList(ks.get_set("prefix", mkSet("02", "02~", 3))).map(_.key) must
					haveTheSameElementsAs(List("0201", "0202", "0203"))
			}

			"descending prefixes" in {
				Conversions.convertList(ks.get_set("prefix", mkSet("02~", "02", 3))).map(_.key) must
					haveTheSameElementsAs(List("0210", "0209", "0208"))
			}

			"descending full ranges" in {
				Conversions.convertList(ks.get_set("prefix", mkSet("~", "", 3))).map(_.key) must
					haveTheSameElementsAs(List("1010", "1009", "1008"))
			}
		}


		"have a get_set function that" >> {
       val records = (3 to 7).toList.map((i) => new Record(keyFormat.format(i), i.toString))
       records.foreach(ks.put("set", _))

			"correctly returns exact ranges" in {
				Conversions.convertList(ks.get_set("set", recSet(3, 7))) must
					haveTheSameElementsAs(records)
			}

			"correctly returns exact ranges backwards" in {
				Conversions.convertList(ks.get_set("set", recSet(7, 3))) must
					haveTheSameElementsAs(records.reverse)
			}

			"correctly returns ranges that extend past both sides of existing keys" in {
				Conversions.convertList(ks.get_set("set", recSet(0, 10))) must
					haveTheSameElementsAs(records)
			}

			"correctly returns ranges that extend past the begining of exisiting keys" in {
				Conversions.convertList(ks.get_set("set", recSet(0, 5))) must
					haveTheSameElementsAs(records.slice(0, 3))
			}

			"correctly returns ranges that extend past the end of existing keys" in {
				Conversions.convertList(ks.get_set("set", recSet(5, 10))) must
					haveTheSameElementsAs(records.slice(2,5))
			}

			"correctly returns empty ranges" in {
				Conversions.convertList(ks.get_set("set", recSet(0, 2))) must
					haveTheSameElementsAs(Array[Record]())
			}

			"respects limit" in {
				val rs = recSet(0,10)
				rs.getRange.setLimit(2)
				Conversions.convertList(ks.get_set("set", rs)) must
					haveTheSameElementsAs(records.slice(0,2))
			}

			"respects offset" in {
				val rs = recSet(0,10)
				rs.getRange.setOffset(1)
				Conversions.convertList(ks.get_set("set", rs)) must
					haveTheSameElementsAs(records.slice(1,5))
			}

			"respects offset with limit" in {
				val rs = recSet(0,10)
				rs.getRange.setOffset(1)
				rs.getRange.setLimit(2)
				Conversions.convertList(ks.get_set("set", rs)) must
					haveTheSameElementsAs(records.slice(1,3))
			}

			"handles -infty to infty ranges" in {
				val rs = recSet(Math.MIN_INT,Math.MAX_INT)
				Conversions.convertList(ks.get_set("set", rs)) must
				haveTheSameElementsAs(records)
			}

			"handles -infty to finite range" in {
				val rs = recSet(Math.MIN_INT,4)
				Conversions.convertList(ks.get_set("set", rs)) must
				haveTheSameElementsAs(records.slice(0,2))
			}
		}
	}
}

object JavaEngineSpec extends KeyStoreSpec {
	BasicConfigurator.configure()
	val dbDir = new File("target/testDb")
	TestableStorageNode.rmDir(dbDir)
	dbDir.mkdir
	val config = new EnvironmentConfig()
	config.setAllowCreate(true)
	config.setTransactional(true)
	val env = new Environment(dbDir, config)
	val ks = new StorageProcessor(env)
}

class JavaEngineTest extends JUnit4(JavaEngineSpec)
