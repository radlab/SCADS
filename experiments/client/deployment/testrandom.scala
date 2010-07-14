import edu.berkeley.cs.scads.deployment.ScadsDeploy

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

import java.util.Random

val storageNodes = Map( r11 -> 9003, r12 -> 9003, r13 -> 9003, r15 -> 9003,
                        r16 -> 9003, r17 -> 9003, r18 -> 9003, r19 -> 9003)
val dataPlacementNode = (r10,8002)

val scadsDeploy = new ScadsDeploy(storageNodes, dataPlacementNode, 2)

scadsDeploy.setDefaultDataBucket(1)

scadsDeploy.deploy
println("Deployed!")

var usernames = getNRandomUsers(16,0)

implicit val env:Environment = scadsDeploy.getEnv

usernames.foreach( (u) => {
    val user = new user
    user.name(u)
    user.save
})

scadsDeploy.rebalance

var usernames2 = getNRandomUsers(16,usernames.length)
usernames2.foreach( (u) => {
    val user = new user
    user.name(u)
    user.save
})

scadsDeploy.rebalance

(usernames ::: usernames2).foreach( (u) => {
    var rtn = Queries.userByName(u)
    if ( rtn.isEmpty ) {
        println("Crap! Could not find user " + u)
        //System.exit(1)
    }
    rtn.foreach(println(_))
})

def getNRandomUsers(numUsers: Int, offset: Int): List[String] = {
    val rGen = new Random()
    val seenBefore = new Array[Boolean](numUsers)
    var usernames: List[String] = Nil
    for ( i <- 0 until numUsers ) {
        var found = false
        var nextInt = 0
        while (!found) {
            nextInt = rGen.nextInt(numUsers)
            if ( !seenBefore(nextInt) ) {
                seenBefore(nextInt) = true
                found = true
            }
        }
        usernames = usernames ::: List("user"+(offset+nextInt))
    }
    usernames
}
