package edu.berkeley.cs.scads.storage

import org.specs._
import org.specs.runner.JUnit4
import scala.collection.jcl.Conversions

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
				val ev = new ExistingValue("n",0)
				ev.unsetValue()
				ev.unsetPrefix()
				val rec = new Record("tasnull","tasnull")
				ks.test_and_set("tasn",rec,ev) must_== true
			}

			"fails when null isn't there" {
				val ev = new ExistingValue("n",0)
				ev.unsetValue()
				ev.unsetPrefix()
				val rec = new Record("tasnullf","tasnullf")

				try {
					fail("Exception not throw when test/set")
				}
				catch {
					case tsf: TestAndSetFailure => tsf.currentValue must_== "tasnullf"
				}
				0
			}

			"succeeds when a prefix is unchanged" in {
				val rec = new Record("tassucp","tassucp1ignore")
				ks.put("tassp",rec)
				val ev = new ExistingValue("tassucp1xxx",7)
				rec.value = "tassucp2"
				ks.test_and_set("tassp",rec,ev) must_== true
				ks.get("tassp","tassucp").value must_== "tassucp2"
			}

			"fail when a prefix has changes" in {
				val rec = new Record("tasfailp","tasfailp1ignore")
				ks.put("tasfp",rec) must_== true
				val ev = new ExistingValue("failval",12)
				rec.value = "tasfailp2"
				try {
					ks.test_and_set("tasfp",rec,ev)
					fail("Didn't throw exception on changed prefix")
				} catch {
					case tsf: TestAndSetFailure => tsf.currentValue must_== "tasfailp1ignore"
				}
				ks.get("tasfp","tasfailp").value must_== "tasfailp1ignore"
			}
		}

		"have a get_set function that" >> {
			val keyFormat = new java.text.DecimalFormat("0000000000000000")
			val records = (10 to 90).toList.map((i) => new Record(keyFormat.format(i), i.toString))
			records.foreach(ks.put("set", _))	

			def recSet(start: Int, end:Int) = {
				val recSet = new RecordSet
				val rangeSet = new RangeSet
				rangeSet.setStart_key(keyFormat.format(start))
				rangeSet.setEnd_key(keyFormat.format(end))
				recSet.setType(RecordSetType.RST_RANGE)
				recSet.setRange(rangeSet)
				recSet
			}


			"correctly returns ranges" in {
				Conversions.convertList(ks.get_set("set", recSet(10, 90))) must
					containInOrder(records)
				Conversions.convertList(ks.get_set("set", recSet(0, 100))) must
					containInOrder(records)
				Conversions.convertList(ks.get_set("set", recSet(0, 50))) must
					containInOrder(records.slice(0, 40))
				Conversions.convertList(ks.get_set("set", recSet(50, 100))) must
					containInOrder(records.slice(40,80))
				Conversions.convertList(ks.get_set("set", recSet(0, 5))) must
					containInOrder(Array[Record]())
			}
		}
	}
}

object JavaEngineSpec extends KeyStoreSpec {
	val dbDir = new File("testDb")
	dbDir.mkdir
	val config = new EnvironmentConfig();
	config.setAllowCreate(true);
	val env = new Environment(new File("testDb"), config)
	val ks = new StorageProcessor(env)
}
