package edu.berkeley.cs.scads.storage

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.thrift._
import java.io.File
import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig


abstract class KeyStoreSpec extends Specification("KeyStore Specification") {
	val ks: KeyStore.Iface

	"a keystore" should {
		"persist values" in {
			val rec = new Record("key", "value")
			ks.put("persistTest", rec) must_== true
			ks.get("persistTest", "key") must_== rec
		}

		"handle updates" in {
			val rec1 = new Record("key", "value1")
			val rec2 = new Record("key", "value2")

			ks.put("updateTest", rec1) must_== true
			ks.put("updateTest", rec2) must_== true
			ks.get("updateTest", "key") must_== rec2
		}

		"delete keys for null values" in {
			val rec = new Record("key", "value")
			val delRec = new Record("key", null)

			ks.put("nullVal", rec) must_== true
			ks.put("nullVal", delRec) must_== true
			ks.get("nullVal", "key") must_== delRec
		}

		"store empty strings" in {
			val rec = new Record("key", "")
			ks.put("emptyString", rec) must_== true
			ks.get("emptyString", "key") must_== rec
		}

		"test and set sucessfully with null values" in {
			val ev = new ExistingValue("n",0)
			ev.unsetValue()
			ev.unsetPrefix()
			val rec = new Record("tasnull","tasnull")
			ks.test_and_set("tasn",rec,ev) must_== true
		}

		"test and set failure with null values in" {
			val ev = new ExistingValue("n",0)
			ev.unsetValue()
			ev.unsetPrefix()
			val rec = new Record("tasnullf","tasnullf")
			ks.put("tasnf",rec)

			try {
				ks.test_and_set("tasnf",rec,ev)
				fail("Exception not throw when test/set")
			}
			catch {
				case tsf: TestAndSetFailure => tsf.currentValue must_== "tasnullf"
			}
			0
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

class JavaEngineTest extends JUnit4(JavaEngineSpec)
