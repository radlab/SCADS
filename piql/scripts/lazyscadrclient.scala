import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

val client = new ScadrClient(TestScalaEngine.getTestCluster, new LazyExecutor with DebugExecutor)
val loader = new ScadrLoader(client, 1, 1)
val data = loader.getData(0)
data.load()
