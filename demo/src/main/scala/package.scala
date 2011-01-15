package edu.berkeley.cs
package radlab

package object demo {
  import DemoConfig._
  import scads.comm._
  import deploylib.mesos._

  def setupMesosMaster: Unit = {
    try MesosEC2.master catch {
      case _ => MesosEC2.startMaster()
    }

    MesosEC2.master.pushJars
    MesosEC2.master.executeCommand("killall java")
    MesosEC2.master.createFile(new java.io.File("/root/serviceScheduler"), "#!/bin/bash\n/root/jrun edu.berkeley.cs.radlab.demo.ServiceSchedulerDaemon >> /root/serviceScheduler.log 2>&1")
    MesosEC2.master ! "chmod 755 /root/serviceScheduler"
    MesosEC2.master ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/serviceScheduler.pid --exec /root/serviceScheduler"
    DemoConfig.serviceSchedulerNode.data = RemoteActor(MesosEC2.master.publicDnsName, 9000, ActorNumber(0)).toBytes
    DemoZooKeeper.root.getOrCreate("demo/mesosMaster").data = MesosEC2.clusterUrl.getBytes
  }

  def startScadr: Unit = {
    val task = WebAppSchedulerTask(
	"SCADr",
	mesosMaster,
	javaExecutorPath,
	scadrWar).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }
}
