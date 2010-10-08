import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

val client = new ScadrClient(TestScalaEngine.getTestCluster, new SimpleExecutor with DebugExecutor)
client.bulkLoadTestData
