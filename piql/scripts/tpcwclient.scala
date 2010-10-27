import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

val client = new TpcwClient(TestScalaEngine.getTestCluster, new SimpleExecutor with DebugExecutor)
val loader = new TpcwLoader(client, 1, 1.0, 10)
val data = loader.getData(0)
data.load()
