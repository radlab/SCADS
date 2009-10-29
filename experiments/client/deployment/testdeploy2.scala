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

scadsDeploy.placeUsers(Array( ("a","h",r13), ("h","z",r15) ))
println("Users Placed!")

implicit val env:Environment = scadsDeploy.getEnv

//println("creating new user")
//val user1 = new user
//println("setting user's name")
//user1.name("b")
//println("saving user")
//user1.save

for ( i <- 0 until numUsers ) {
    val user = new user
    val x = i / 26
    val y = i % 26
    val c:Char = (0x61+y).toChar
    val s = (new String(Array(c)))*(x+1)
    println("Saving user with username: " + s)
    user.name(s)        
    user.save
}

var rtn = Queries.userByName("a")
println("running query:")
rtn.foreach(println(_))

rtn = Queries.userByName("k")
println("running query:")
rtn.foreach(println(_))

rtn = Queries.userByName("x")
println("running query:")
rtn.foreach(println(_))

scadsDeploy.shutdown
println("Stopped!")
