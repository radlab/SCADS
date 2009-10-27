import edu.berkeley.cs.scads.deployment.ScadsDeploy

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

val storageNodes = Map( r13 -> 9003, r15 -> 9003 )
val dataPlacementNode = (r10,8002)

val scadsDeploy = new ScadsDeploy(storageNodes, dataPlacementNode)

scadsDeploy.deploy
println("Deployed!")

scadsDeploy.placeUsers(Array( ("a","h",r13), ("h","z",r15) ))
println("Users Placed!")

implicit val env:Environment = scadsDeploy.getEnv

println("creating new user")
val user1 = new user
println("setting user's name")
user1.name("b")
println("saving user")
user1.save

val rtn = Queries.userByName("b")
println("running query:")
rtn.foreach(println(_))

scadsDeploy.shutdown
println("Stopped!")
