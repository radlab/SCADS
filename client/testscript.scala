import edu.berkeley.cs.scads.model.TrivialExecutor
import edu.berkeley.cs.scads.model.TrivialSession
import edu.berkeley.cs.scads.TestCluster
import org.apache.log4j.BasicConfigurator

BasicConfigurator.configure()

implicit val env = ScadsEnv

ScadsEnv.placement = new TestCluster
ScadsEnv.session = new TrivialSession
ScadsEnv.executor = new TrivialExecutor()(ScadsEnv)

val u = new user
u.name("marmbrus")
u.email("marmbrus@cs.berkeley.edu")
u.save

println(Queries.userByName("marmbrus").apply(0).email)
