import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

val client = new ScadrClient(TestScalaEngine.getTestCluster, new ParallelExecutor with DebugExecutor)
client.bulkLoadTestData
