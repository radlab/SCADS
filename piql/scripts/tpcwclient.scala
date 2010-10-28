import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

def init(numNodes: Int) = {
  val cluster = TestScalaEngine.getTestCluster
  TestScalaEngine.getTestHandler(numNodes - 1)
  val client = new TpcwClient(cluster, new SimpleExecutor with DebugExecutor)
  val loader = new TpcwLoader(client, 1, 1.0, 10)
  loader.createNamespaces()
  val data = loader.getData(0)
  data.load()
  (client, loader)
}
