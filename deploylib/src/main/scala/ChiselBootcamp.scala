package chisel

import deploylib._
import deploylib.ec2._

import java.net.InetAddress
import java.util.Properties._
import javax.mail._
import javax.mail.internet._
import scala.collection.JavaConversions._
import java.io.File

object Bootcamp {
  val region = USWest2
  val imageId = USWest2.defaultAMI
  val instanceTag = ("chisel", "bootcamp")
  val userTag = "chiselUser"

  val addressFile = new File("emails.txt")
  val addresses = io.Source.fromFile(addressFile).getLines.toSeq

  //Start a few extra instances in case there are stragglers / failures.
  val instanceCount = addresses.size + 10

  System.setProperty("mail.smtp.host", "localhost")

  def startInstances: Unit = {
    val startCount = instanceCount - unusedInstances.size
    if(startCount > 0) {
      val instances = region.runInstances(imageId, instanceCount, instanceCount, region.keyName, "m1.medium")
      instances.foreach(_.tags += instanceTag)
    }
  }

  def activeInstances = region.activeInstances.filter(_.tags.find(_.getKey equals "chisel").isDefined)

  def unusedInstances = activeInstances.filterNot(_.tags.find(_.getKey equals userTag).isDefined)

  def sendEmail(inst: EC2Instance, address: String): Unit = {
    val session = Session.getDefaultInstance(System.getProperties)
    val message = new MimeMessage(session)

    message.setFrom(new InternetAddress("marmbrus@cs.berkeley.edu"))
    message.setRecipients(Message.RecipientType.TO, address)
    message.setSubject("Chisel Bootcamp: Tutorial VM Login Information")
    message.setText("ssh ubuntu@%s\npassword: bootcamp".format(inst.publicDnsName))

    println("%s\t%s".format(address, inst.publicDnsName))

    Transport.send(message)
  }

  def run: Unit = {
    startInstances

    while(activeInstances.size < addresses.size) {
      println("Waiting for instances to start. %d of %d ready.".format(activeInstances.size, addresses.size))
      Thread.sleep(5 * 1000)
    }

    val instances = unusedInstances
    instances.take(addresses.size).zip(addresses).foreach {
      case (inst, addr) =>
        inst.tags += (userTag, addr)
        sendEmail(inst,addr)
    }

    println("Unused Instances:")
    unusedInstances.map(_.publicDnsName).foreach(println)
  }
}