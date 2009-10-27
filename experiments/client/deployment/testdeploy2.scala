import edu.berkeley.cs.scads.deployment.ScadsDeploy

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

val storageNodes = Array( (r13,9002), (r15,9002) )
val dataPlacementNode = (r10,8001)

val scadsDeploy = new ScadsDeploy(storageNodes, dataPlacementNode)

scadsDeploy.deploy
println("Deployed!")
scadsDeploy.shutdown
println("StoppedQ!")
