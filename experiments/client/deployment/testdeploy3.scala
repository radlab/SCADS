import edu.berkeley.cs.scads.deployment.ScadsDeploy

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

val storageNodes = Map( r11 -> 9003, r13 -> 9003, r15 -> 9003 )
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

usernames.foreach( (u) => {
    var rtn = Queries.userByName(u)
    if ( rtn.isEmpty ) {
        println("Crap! Could not find user " + u) 
        System.exit(1)
    }
    rtn.foreach(println(_))
})

val numThoughts = 100
var thoughts: List[Int] = Nil
for ( i <- 1 until (numThoughts+1) ) {
    thoughts = thoughts ::: List(i)
}
scadsDeploy.equalKeyPartitionThoughts(thoughts)

thoughts.foreach( (timestamp) => {
    val thought = new thought
    thought.timestamp(timestamp)
    thought.save
})


scadsDeploy.shutdown
println("Stopped!")
