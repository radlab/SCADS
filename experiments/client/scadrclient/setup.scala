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

val u2 = new user
u2.name("sltu")
u2.password("pass")
u2.email("sltu@cs.berkeley.edu")
u2.save

val s = new subscription
s.approved(true)
s.owner("marmbrus")
s.target("sltu")
s.save

val t1 = new thought
t1.owner(u1)
t1.timestamp(1)
t1.thought("michael: Hey there scadr world!")
t1.save

val t2 = new thought
t2.owner(u2)
t2.timestamp(1)
t2.thought("stephen: Hey there from me too!")
t2.save
