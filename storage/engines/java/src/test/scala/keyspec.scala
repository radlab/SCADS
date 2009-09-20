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
			ks.put("persistTest", rec)
			ks.get("persistTest", "key") must_== rec
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
