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
