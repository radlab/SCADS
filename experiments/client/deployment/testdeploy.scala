import edu.berkeley.cs.scads.deployment._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._

val storageMachines = List( (r11,9003), (r12,9003), (r13,9003), (r15,9003) ).map( (t) => StorageMachine.fromTuple(t) )
val dataPlacementMachine = DataPlacementMachine.fromTuple( (r10,8002) )

val scadsDeploy = new ScadsDeploy2(storageMachines, dataPlacementMachine, 1)

scadsDeploy.deploy

val numUsers = 16

var usernames: List[String] = Nil
for ( i <- 0 until numUsers ) {
    usernames = usernames ::: List("user"+i)
}

implicit val env:Environment = scadsDeploy.getEnv

usernames.foreach( (u) => {
    val user = new user
    user.name(u)
    user.save
})

scadsDeploy.rebalance

val numUsersRoundTwo = 16
var usernames2: List[String] = Nil
for ( i <- numUsers until (numUsers+numUsersRoundTwo) ) {
    usernames2 = usernames2 ::: List("user"+i)
}
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
