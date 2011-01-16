package edu.berkeley.cs
package radlab

import java.io.File

package object demo {
  import DemoConfig._
  import scads.comm._
  import deploylib.mesos._

  /**
   * Start a mesos master and make it the primary for the demo.
   * Only needs to be run by one person.
   */
  def setupMesosMaster: Unit = {
    try MesosEC2.master catch {
      case _ => MesosEC2.startMaster()
    }

    MesosEC2.master.pushJars
    restartServiceScheduler
    mesosMasterNode.data = MesosEC2.clusterUrl.getBytes
  }

  /**
   * Restart the service meta-scheduler on the mesos master.
   * Note this should only be run by the cluster owner, and it will kill all running jobs.
   */
  def restartServiceScheduler: Unit = {
    MesosEC2.master.executeCommand("killall java")
    MesosEC2.master.createFile(new java.io.File("/root/serviceScheduler"), "#!/bin/bash\n/root/jrun edu.berkeley.cs.radlab.demo.ServiceSchedulerDaemon >> /root/serviceScheduler.log 2>&1")
    MesosEC2.master ! "chmod 755 /root/serviceScheduler"
    MesosEC2.master ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/serviceScheduler.pid --exec /root/serviceScheduler"
    //HACK
    Thread.sleep(5000)
    serviceSchedulerNode.data = RemoteActor(MesosEC2.master.publicDnsName, 9000, ActorNumber(0)).toBytes
  }

  def startScadr: Unit = {
    val task = WebAppSchedulerTask(
	"SCADr",
	mesosMaster,
	javaExecutorPath,
	scadrWar).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startScadrDirector: Unit = {
    val task = ScadrDirectorTask(
      scadrRoot.canonicalAddress,
      mesosMaster
    ).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  /**
   * WARNING: deletes all data from all scads cluster
   */
  def resetScads: Unit = {
    val namespaces = "users" :: "thoughts" :: "subscriptions" :: Nil
    val delCmd = "rm -rf " + namespaces.map(ns => "/mnt/" + ns + "*").mkString(" ")
    MesosEC2.slaves.pforeach(_ ! delCmd)

    scadrRoot.deleteRecursive
  }

  def startIntKeyTest: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(MesosEC2.classSource,
		  "edu.berkeley.cs.radlab.demo.IntKeyScaleScheduler",
		  "--name" :: "intkeyscaletest" ::
		  "--mesosMaster" :: mesosMaster ::
		  "--executor" :: javaExecutorPath ::
		  "--cp" :: MesosEC2.classSource.map(_.url).mkString(":") :: Nil) :: Nil
    )
  }

  def startRepTest: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(MesosEC2.classSource,
		  "edu.berkeley.cs.radlab.demo.RepTestScheduler",
		  "--name" :: "reptest" ::
		  "--mesosMaster" :: mesosMaster ::
		  "--executor" :: javaExecutorPath ::
		  "--cp" :: MesosEC2.classSource.map(_.url).mkString(":") :: Nil) :: Nil
    )
  }

  def authorizeUsers: Unit = {
    val keys = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCfH8CkLrCIOxJAkFubG1ehQEdu1OfOUqaMxiTQ7g/X0fXclXRzqwoBFBL33t0FGVxkPVxolwAaZEQTIg6hkGZuzLlPiuq1ortkMx3wGxU9/YBr6JzSZb+kB1OEG/LOWiXH+i5IJbKptW+6B527niXCAgo8Idlf5PNBqcdI+CrvaX+oqQX6K2T5EDxoJVOtgRHbS/2YbtGhwknskyCcvOnOcwjcRUGawmVK7QYavyuO+//SOK+0sIjTSSwTAVceKbQl8XVlPL7IJHKE6/JwEF2+6+eMdflg9A8qAm3g0rE8qfUGdJLN1hpJNdP/UCP1v091h4C88lqqtwbekrS817ar stephentu@ibanez" :: Nil

    keys.foreach(k => MesosEC2.master.appendFile(new File("/root/.ssh/authorized_keys"), k))
  }
}
