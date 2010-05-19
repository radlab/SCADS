//import edu.berkeley.cs.scads.model.Environment
//import edu.berkeley.cs.scads.model.{TrivialExecutor,TrivialSession, TestCluster}
//import org.apache.log4j._
//import org.apache.log4j.Level._

import org.apache.avro.specific.SpecificRecordBase
import edu.berkeley.cs.scads.storage.{Namespace, TestScalaEngine}

import piql._

implicit val env = (new Configurator).configureTestCluster

val u1 = new User
u1.key.name = "marmbrus"
u1.value.password = "pass"
u1.value.email = "marmbrus@berkeley.edu"
u1.save

val u2 = new User
u2.key.name = "sltu"
u2.value.password = "pass"
u2.value.email = "sltu@cs.berkeley.edu"
u2.save

val s = new Subscription
s.value.approved = true
s.key.owner.name = "marmbrus"
s.key.target.name = "sltu"
s.save

val t1 = new Thought
t1.key.owner.name = u1.name
t1.key.timestamp = 1
t1.value.text = "michael: Hey there scadr world!"
t1.save

val t2 = new Thought
t2.key.owner.name = u2.name
t2.key.timestamp = 1
t2.value.text = "stephen: Hey there from me too!"
t2.save

val marmbrus = Queries.userByName("marmbrus").first
println(marmbrus.myFollowing(10))

val sltu = Queries.userByName("sltu").first
println(sltu.myFollowing(10))



//val h1 = new hashTag
//h1.name("michael")
//h1.referringThought(t1)
//h1.save
//
//val h2 = new hashTag
//h2.name("stephen")
//h2.referringThought(t2)
//h2.save
//
//val h3 = new hashTag
//h3.name("hello")
//h3.referringThought(t1)
//h3.save
//
//val h4 = new hashTag
//h4.name("hello")
//h4.referringThought(t2)
//h4.save
