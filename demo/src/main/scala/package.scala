package edu.berkeley.cs
package radlab

package object demo {
  import DemoConfig._
  import scads.comm._
  import deploylib.mesos._

  def fixServiceSchedulerPid: Unit = {
    DemoConfig.serviceSchedulerNode.data = RemoteActor(MesosEC2.master.publicDnsName, 9000, ActorNumber(0)).toBytes
  }

  def startScadr: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(MesosEC2.classSource,
		  "edu.berkeley.cs.radlab.WebAppScheduler",
		  "--name" :: "SCADr" ::
		  "--mesosMaster" :: MesosEC2.clusterUrl ::
		  "--executor" :: javaExecutorPath ::
		  "--warFile" :: scadrWar :: Nil) :: Nil
    )
  }
}
