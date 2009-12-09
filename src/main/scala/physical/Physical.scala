package deploylib.physical

import java.io.File
import java.net.InetAddress
import java.net.DatagramPacket
import java.net.DatagramSocket
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
    def getMacAddress( machine: PhysicalInstance ) : String = {
        val cmd: String = "/bin/ping -c 5 -q " +  machine.hostname + " && /usr/sbin/arp -a | /bin/grep " + machine.hostname + " | awk '{print $4}'"
        val response: ExecuteResponse = machine.executeCommand( cmd )
        response.stdout;
    }

    def sendWolPacket( broadcastAddress: String, macAddress: String, port: Int ) : Boolean = {
        val macParts:Array[String] = macAddress.split ( ":|-" )
        
        val macBytes:Array[Byte] = for{ e <- macParts
            hex_val:Byte = Integer.parseInt( e, 16 ).asInstanceOf[Byte]
        } yield hex_val;

        for{ b <- macBytes } println( Integer.toHexString(b) )

        val wolPacket: Array[Byte] = new Array[Byte](6 + 16*macBytes.length)

        // Fill in the wolPacket: 6 bytes of all 1's and 16 copies of the mac address
        for{ i <- 0 to 5 } wolPacket(i) = 0xff.asInstanceOf[Byte]
        var i = 6;
        while( i < wolPacket.length )
        {
            System.arraycopy( macBytes, 0, wolPacket, i, macBytes.length )
            i += macBytes.length
        }
        
        // Send the WOL packet
        val ipAddress: InetAddress = InetAddress.getByName( broadcastAddress )
        val pkt:DatagramPacket = new DatagramPacket( wolPacket, wolPacket.length, port )
        val socket:DatagramSocket = new DatagramSocket()
        socket.send( pkt )
        socket.close()
        
        return true;
    }

    def saveMacAddress( machine: PhysicalInstance ) : Unit = {
        val mac:String = getMacAddress(machine);
        machine.mac = mac;
        machine.tagMachine( "mac-discovered" );
    }

    def machineSleep( machine: PhysicalInstance ) = {
        // See if the machine is tagged as sleeping - if not then
        // ssh in and put it to sleep
        // If there's a daemon running send it a sleep packet
        machine.executeCommand( "echo -n \"mem\" > /sys/power/state" )
        // tag the machine as sleeping
        machine.tagMachine( "sleeping" )
    }

    def machineWake( machine: PhysicalInstance, timeout: Long ): Boolean = {
        // See if the machine is tagged as sleeping - if it is then
        // send it a wakeOnLan packet
        // Make sure it is alive
        // Remove the sleeping tag
        // Send the WOL packet on port 0, 7 or 9
        sendWolPacket( machine.broadcastAddress, machine.mac, 9 )
        // Ping machine - with some timeout limit
        if( machinePing( machine, timeout ) ) {
            machine.removeTag( "sleeping" )
            return true
        }
        
        false
    }

    // Try to contact a machine, wait on the response, if the ping works
    // return true, else return false
    def machinePing( machine:PhysicalInstance, timeout: Long ) : Boolean = {
        // get the hostname and run the cmd "ping hostname"
        // Look for a 100% loss
        true
    }

    def isSleeping( machine:PhysicalInstance, timeout: Long ) : Boolean = {
        machine.isTagged( "sleeping" ) && !machinePing( machine, timeout )
    }
}

class PhysicalInstance(val hostname: String, val username: String, val privateKey: File) extends RemoteMachine
{
  val rootDirectory: File = new File("/mnt/")
  val runitBinaryPath:File = new File("/usr/bin")
  val tags: HashMap[String,Tag] = new HashMap[String,Tag];
  var mac: String = "";
  var broadcastAddress = "";

  def this( hostname: String ) = {
      this( hostname, "root", new File("/Users/marmbrus/.ec2/amazon/") )
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
    def main(args:Array[String]) = {
        // 00:1c:c0:c2:7a:85
        val mac:String = "00:1c:c0:c2:7a:85"
        
        // List all the nodes we have in the loCal cluster
        val a9 = ( "192.168.0.21", "root", new File("/home/user/.ssh/atom_key") )
        val a14 = ( "192.168.0.27", "root", new File("/home/user/.ssh/atom_key") )
        val a15 = ( "192.168.0.28", "root", new File("/home/user/.ssh/atom_key") )
        val a16 = ( "192.168.0.29", "root", new File("/home/user/.ssh/atom_key") )

        // Initialize the cluster
        val atoms = List(a9,a14,a15,a16).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        var atom9:PhysicalInstance = atoms{0};
        var atom14:PhysicalInstance = atoms{1};
        var atom15:PhysicalInstance = atoms{2};
        var atom16:PhysicalInstance = atoms{3};


        // For each machine, deploy all the services based on the tags
        // if services exist that correspond to a tag name
        // e.g. there will be a rails service for machines tagged with "rails"
        // but there won't be a "sleeping" service for machines tagged with "sleeping"

        // Service.start(PhysicalInstance), Service.stop(PhysicalInstance),
        // Service.restart(PhysicalInstance), Service.startOnce(PhysicalInstance)
        // Service.configure, Service.toJSON()
        // Cluster.InstanceSleep(PhysicalInstance)
        // Cluster.InstanceWake(PhysicalInstance)
        
        println("hello-loCal-xxxx");
    }
}
