import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
import edu.berkeley.cs.scads.TestCluster
import org.apache.log4j._
import org.apache.log4j.Level._


implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor

val u1 = new user
u1.name("marmbrus")
u1.password("pass")
u1.email("marmbrus@berkeley.edu")
u1.save
