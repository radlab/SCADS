package edu.berkeley.cs.scads.test

import org.specs._
import org.specs.runner.JUnit4

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.comm.Conversions._

import edu.berkeley.cs.scads.storage._

object KeyStoreSpec extends SpecificationWithJUnit("KeyStore Specification") {
	implicit val proxy = new StorageActorProxy
	
	val intRec = new IntRec
	TestScalaEngine.cluster.createNamespace("KeySpecInt", intRec.getSchema(), intRec.getSchema())

	val cr = new ConfigureRequest
	cr.namespace = "KeySpecInt"
	cr.partition = "1"
	Sync.makeRequest(TestScalaEngine.node, cr)

	"a keystore" should {
		"store key/value pairs such that" >> {
			"updates persist" in {
				val k1 = new IntRec
				val v1 = new IntRec
				k1.f1 = 1
				v1.f1 = 1

				val pr = new PutRequest
				pr.namespace = "KeySpecInt"
				pr.key = k1.toBytes
				pr.value = v1.toBytes 
				Sync.makeRequest(TestScalaEngine.node, pr)

				val gr = new GetRequest
				gr.namespace = "KeySpecInt"
				gr.key = k1.toBytes
				val res = Sync.makeRequest(TestScalaEngine.node, gr).asInstanceOf[Record]
				val k2 = new IntRec
				val v2 = new IntRec


				println(res.key)
				println(res.value)

				k2.parse(res.key)
				v2.parse(res.value)

				k1 must_== k2
				v1 must_== v2
			}
		}
	}
}

class KeyStoreTest extends JUnit4(KeyStoreSpec)
