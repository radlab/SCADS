import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession}
import edu.berkeley.cs.scads.TestCluster
import org.apache.log4j._
import org.apache.log4j.Level._


implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor

(1 to 1000).foreach((i) => {
	val u = new user
	u.name("user" + i)
	u.password("secret")
	u.email("user" + i + "@test.com")
	u.save
})
