import edu.berkeley.cs.scads.model.TrivialExecutor
import edu.berkeley.cs.scads.model.TrivialSession
import edu.berkeley.cs.scads.model.Environment
import edu.berkeley.cs.scads.TestCluster
import org.apache.log4j.BasicConfigurator

BasicConfigurator.configure()

implicit val env = new Environment
env.placement = new TestCluster
env.session = new TrivialSession
env.executor = new TrivialExecutor

val u = new user
u.name("marmbrus")
u.email("marmbrus@cs.berkeley.edu")
u.save

val t = new thought
t.timestamp(1)
t.save

println(Queries.userByName("marmbrus").apply(0).email)
