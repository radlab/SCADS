import edu.berkeley.cs.radlab.demo._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.scads.piql.gradit._
import edu.berkeley.cs.scads.piql.scadr._
import edu.berkeley.cs.scads.piql.comrades._
import edu.berkeley.cs._
import deploylib.mesos._

lazy val testGraditClient = new GraditClient(TestScalaEngine.newScadsCluster(), new DashboardReportingExecutor())
lazy val testScadrClient = new ScadrClient(TestScalaEngine.newScadsCluster(), new DashboardReportingExecutor())
lazy val testComradesClient = new ComradesClient(TestScalaEngine.newScadsCluster(), new DashboardReportingExecutor())
