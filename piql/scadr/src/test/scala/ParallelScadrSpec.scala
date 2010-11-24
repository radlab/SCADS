package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

@RunWith(classOf[JUnitRunner])
class ParallelScadrSpec extends AbstractScadrSpec {
  lazy val client = new ScadrClient(TestScalaEngine.getTestCluster, new ParallelExecutor with DebugExecutor)
}
