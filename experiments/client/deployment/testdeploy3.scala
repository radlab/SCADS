import edu.berkeley.cs.scads.deployment.ScadsDeploy

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

val storageNodes = Map( r13 -> 9003, r15 -> 9003 )
val dataPlacementNode = (r10,8002)

val numUsers = 100

val scadsDeploy = new ScadsDeploy(storageNodes, dataPlacementNode)

scadsDeploy.deploy
println("Deployed!")

var usernames: List[String] = Nil
for ( i <- 0 until numUsers ) {
    usernames = usernames ::: List("user"+i)
}

scadsDeploy.equalKeyPartitionUsers(usernames)

implicit val env:Environment = scadsDeploy.getEnv

usernames.foreach( (u) => {
    val user = new user
    user.name(u)
    user.save
})

var rtn = Queries.userByName("user10")
println("running query:")
rtn.foreach(println(_))


scadsDeploy.shutdown
println("Stopped!")
