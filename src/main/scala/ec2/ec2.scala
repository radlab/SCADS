package deploylib.ec2

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._
import java.io.File

import scala.collection.jcl.Conversions._

object EC2 {
	private val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  private val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")

  var keyName = System.getenv("AWS_KEY_NAME")
  var keyPath = System.getenv("AWS_KEY_PATH")

	private val config = new AmazonEC2Config()

  if(System.getenv("EC2_URL") != null)
  	config.setServiceURL(System.getenv("EC2_URL"))

  val client = new AmazonEC2Client(accessKeyId, secretAccessKey, config)
	val instances = new scala.collection.mutable.HashMap[String, RunningInstance]

	update()

	def update():Unit = {
		val result = client.describeInstances(new DescribeInstancesRequest()).getDescribeInstancesResult()
		result.getReservation.foreach((r) => {
			r.getRunningInstance.foreach((i) => {
				instances.put(i.getInstanceId, i)
			})
		})
	}

	def activeInstances: List[EC2Instance] = {
		update()
		instances.keys.map(new EC2Instance(_)).filter(_.instanceState equals "running").toList
	}

	def runInstance(): EC2Instance =
		runInstances("ami-e7a2448e", 1, 1, "marmbrus", "m1.small", "us-east-1a")(0)

	def runInstances(imageId: String, min: Int, max: Int, keyName: String, instanceType: String, location: String): Seq[EC2Instance] = {
		val request = new RunInstancesRequest(
												imageId,                 // imageID
                        min,                     // minCount
                        max,                     // maxCount
                        keyName,                 // keyName
                        null,                    // securityGroup
                        null,                    // userData
                        instanceType,            // instanceType
                        new Placement(location), // placement
                        null,                    // kernelId
                        null,                    // ramdiskId
                        null,                    // blockDeviceMapping
                        null)                    // monitoring

		val result = client.runInstances(request).getRunInstancesResult()
		result.getReservation().getRunningInstance().map(ri => {
			instances.put(ri.getInstanceId, ri)
			new EC2Instance(ri.getInstanceId())
		})
	}
}

class EC2Instance(instId: String) extends RemoteMachine {
	lazy val hostname: String = getHostname()
	val username: String = "root"
	val privateKey: File = new File("/Users/marmbrus/.ec2/amazon/marmbrus.key")
	val rootDirectory: File = new File("/mnt/")
	val runitBinaryPath:File = new File("/usr/bin")
	val instanceId = instId

	def halt: Unit =
		executeCommand("halt")

	def currentState: RunningInstance =
		EC2.instances(instanceId)

	def imageId: String =
		currentState.getImageId()

	def publicDnsName: String =
		currentState.getPublicDnsName()

	def privateDnsName: String =
		currentState.getPrivateDnsName()

	def keyName: String =
		currentState.getKeyName()

	def instanceType: String =
		currentState.getInstanceType()

	def availabilityZone: String =
		currentState.getPlacement().getAvailabilityZone()

	def instanceState: String =
		EC2.instances(instanceId).getInstanceState.getName()

	def getHostname(): String = {
		waitUntilRunning()
		publicDnsName
	}

	def waitUntilRunning():Unit = {
		while(instanceState equals "pending"){
			logger.info("Waiting for instance " + this)
			Thread.sleep(10000)
			EC2.update()
		}
	}

  override def equals(other: Any): Boolean = other match {
    case that: EC2Instance => instanceId.equals(that.instanceId)
    case _ => false
  }

  override def hashCode: Int = instanceId.hashCode

	override def toString(): String = "<EC2Instance " + instanceId + ">"
}
