import org.scalatest.Suite
import edu.berkeley.cs.scads.AutoKey._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.TestableStorageNode

abstract class KeyStoreSuite extends Suite {
  def getKeyStore: KeyStore.Iface

  def testSimpleGetPut() {
    getKeyStore.put("simpleGetPut", new Record("test", "test"))
  }
}

class BdbKeyStoreTest extends KeyStoreSuite {
  val n = new TestableStorageNode()
  
  def getKeyStore = n.getClient()
}