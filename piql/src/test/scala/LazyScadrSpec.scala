package edu.berkeley.cs.scads.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import edu.berkeley.cs.scads.storage.TestScalaEngine
import edu.berkeley.cs.scads.piql._

@RunWith(classOf[JUnitRunner])
class LazyScadrSpec extends AbstractScadrSpec {
  lazy val client = new ScadrClient(TestScalaEngine.getTestCluster, new LazyExecutor with DebugExecutor)
}
