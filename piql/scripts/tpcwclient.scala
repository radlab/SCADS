import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

def init(numNodes: Int, numLoaders: Int, numEBs: Double = 1.0, numItems: Int = 10) = {
  require(numNodes > 0)
  require(numLoaders > 0)

  val cluster = TestScalaEngine.getTestCluster
  TestScalaEngine.getTestHandler(numNodes - 1)
  val client = new TpcwClient(cluster, new SimpleExecutor with DebugExecutor)
  val loader = new TpcwLoader(client, numLoaders, numEBs, numItems)
  loader.createNamespaces()
  (0 until numLoaders).foreach(id => {
    val data = loader.getData(id)
    data.load()
  })
  (client, loader)
}
