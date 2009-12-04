package deploylib.physical

import java.io.File
import deploylib._
import scala.collection.jcl._
import scala.collection.jcl.Conversions._
import scala.collection.mutable.Map
/*object Physical
{
  var keyName = "marmbrus.key"
  var keyPath = "/Users/marmbrus/.ec2/amazon/"
}*/
object cluster {
    def saveMACAddress( machine: PhysicalInstance, interface: String ) : Unit = {
        // /bin/ping -c 5 -q <ipaddress> && /usr/sbin/arp -a | /bin/grep <ipaddress> | awk '{print $4}'
        // Ping the machine 
        // Then run arp -a | grep <ip address> | awk '{print $4}'
        // Get the MAC address
        // save the MAC
        val cmd: String = "/bin/ping -c 5 -q " +  machine.hostname + " && /usr/sbin/arp -a | /bin/grep " + machine.hostname + " | awk '{print $4}'"
        val response: ExecuteResponse = machine.executeCommand( cmd )
        // Pattern match for the MAC address [2 chars 0-9 a-f]:[]:[]:[]
        println( response.stdout )
    }

    def machineSleep( machine: PhysicalInstance ) = {
        // See if the machine is tagged as sleeping - if not then
        // ssh in and put it to sleep
        // If there's a daemon running send it a sleep packet
        machine.executeCommand( "echo -n \"mem\" > /sys/power/state" )
        // tag the machine as sleeping
        machine.tagMachine( "sleeping" )
    }
    
    def machineWake( machine: PhysicalInstance ) = {
        // See if the machine is tagged as sleeping - if it is then
        // send it a wakeOnLan packet
        // Make sure it is alive
        // Remove the sleeping tag
        machine.removeTag( "sleeping" )
    }

    // Try to contact a machine, wait on the response, if the ping works
    // return true, else return false
    def machinePing( machine:PhysicalInstance, timeout: Long) : Boolean = {
        // get the hostname and run the cmd "ping hostname"
        // Look for a 100% loss
        true
    }

    def isSleeping( machine:PhysicalInstance, timeout: Long ) : Boolean = {
        machine.isTagged( "sleeping" ) && !machinePing( machine, timeout )
    }
}

class PhysicalInstance(val hostname: String) extends RemoteMachine
{
  val username: String = "root"
  val privateKey: File = new File("/Users/marmbrus/.ec2/amazon/")
  val rootDirectory: File = new File("/mnt/")
  val runitBinaryPath:File = new File("/usr/bin")
  val tags: HashMap[String,Tag] = new HashMap[String,Tag];
  var mac: String = "";

  def this( hostname: String, username: String, privateKey: File ) =
  {
      this( hostname )
  }

  def tagMachine( tag: String, persistent: Boolean ) : Unit = {
      if( !tags.contains(tag) )
      {
          println( "Tagging" );
          val newTag: Tag = new Tag( tag, persistent );
          this.tags.put( newTag.name, newTag );
      }
      else println("already tagged")
  }

  def tagMachine( tag: String ) : Unit = {
      tagMachine( tag, false )
  }

  def isTagged( tag: String ): Boolean = {
    return tags.contains(tag)
  }

  def getTag( tag: String ) : Option[Tag] = {
    tags.get(tag)
  }

  def getAllTags() : Map[String,Tag] = {
      return tags
  }

  def removeTag( tag:String ) = {
    tags.remove( tag )
  }
}

object clusterDriver
{
    // List all the nodes we have in the loCal cluster
    val n1 = ("1.1.1.0","user",new File("/tmp/key"));
    val n2 = ("1.1.1.1","user",new File("/tmp/key"))
    val n3 = ("1.1.1.2","user",new File("/tmp/key"))
    val n4 = ("1.1.1.3","user",new File("/tmp/key"))
    val n5 = ("1.1.1.4","user",new File("/tmp/key"))
    val n6 = ("1.1.1.5","user",new File("/tmp/key"))
    val n7 = ("1.1.1.6","user",new File("/tmp/key"))
    val n8 = ("1.1.1.7","user",new File("/tmp/key"))
    val n9 = ("1.1.1.8","user",new File("/tmp/key"))

    val a9 = ( "192.168.0.21", "root", new File("/home/user/.ssh/atom_key") )

    def main(args:Array[String]) = {
        // Initialize the cluster
        val nodes: List[PhysicalInstance] = List(n1,n2,n3,n4,n5,n6,n6,n8,n9).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        val atoms = List(a9).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        var atom9:PhysicalInstance = atoms{0};


        // Pick a node and tag it
        var nodeA:PhysicalInstance = nodes{0};
        var nodeB:PhysicalInstance = nodes{4};

        // Pick a few machines to assign roles

        // For each machine, deploy all the services based on the tags
        // if services exist that correspond to a tag name
        // e.g. there will be a rails service for machines tagged with "rails"
        // but there won't be a "sleeping" service for machines tagged with "sleeping"

        // Service.start(PhysicalInstance), Service.stop(PhysicalInstance),
        // Service.restart(PhysicalInstance), Service.startOnce(PhysicalInstance)
        // Service.configure, Service.toJSON()
        // Cluster.InstanceSleep(PhysicalInstance)
        // Cluster.InstanceWake(PhysicalInstance)

        // Tag nodes - may be transient or persistent
        nodeA.tagMachine( "rails", true );
        nodeB.tagMachine( "rails", true );
        nodeA.tagMachine( "rails", true );

        // Find rails nodes
        val railsNodes = for { node <- nodes
            if( node.isTagged( "rails" ) )
        } yield node;

        nodeA.getTag( "mysql" ) match {
            case Some(tag) => println( "matched " + tag.name )
            case None => println( "no match" )
        }

        


        println( railsNodes.length + " rails nodes found" )

        println("hello-loCal-xxxx");
    }
}
