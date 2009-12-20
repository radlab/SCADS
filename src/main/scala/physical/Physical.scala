package deploylib.physical

import java.io._
import java.net.InetAddress
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.io.File
import deploylib._
import scala.collection.jcl._
import scala.collection.jcl.Conversions._
import scala.collection.mutable.Map
import scala.util.matching.Regex

object cluster {
    val DEFAULT_WOL_PORT: Int = 9
    val DEFAULT_NUM_PING_PACKETS: Int = 3

    // Run command locally instead of on a remote machine
    def executeLocalCommand( cmd: String ) : ExecuteResponse = {
        var exitStatus:java.lang.Integer = null
        val stdout = new StringBuilder
    	val stderr = new StringBuilder
        try
        {
            val proc: Process = Runtime.getRuntime().exec( cmd )
            val outReader = new BufferedReader(new InputStreamReader(proc.getInputStream()))
            val errReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()))
            // Wait for the process to finish
            proc.waitFor();
            // Clear stderr
            while(errReader.ready) {
                val line = errReader.readLine()
                if(line != null) {
                    stderr.append(line)
                    stderr.append("\n")
                }
            }
            // Clear stdout
            while(outReader.ready) {
                val line = outReader.readLine()
                if(line != null) {
                    stdout.append(line)
                    stdout.append("\n")
                }
            }
            // Save the exit status
            exitStatus = proc.exitValue()
            ExecuteResponse(Some(exitStatus.intValue), stdout.toString(), stderr.toString())
        }
        catch
        {
            case e:Exception => ExecuteResponse(Some(-1), stdout.toString(), e.toString())
        }
    }

    
    def getMacAddress( hostname: String ) : String = {
        // Ping the machine and if it works use arp to get mac address
        if( machinePing( hostname, cluster.DEFAULT_NUM_PING_PACKETS ) ) {
            val result: ExecuteResponse = executeLocalCommand( "/usr/sbin/arp -a " + hostname )
            if( result.status.isEmpty || result.status.get != 0 )
                return ""
            // Pull out what should be the MAC address - we need to make
            // sure it matches the format of a MAC address in case we get an
            // ARP miss (which will also result in a status code of 0)
            val mac: String = result.stdout.split( " " ){3}
            if( mac.contains(":") ) // cheap hack looking for mac address separator
                return mac
            else return ""
        }
        return ""
    }

    def getMacAddress( machine: PhysicalInstance ) : String = {
        return getMacAddress( machine.hostname )
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
        val socket:DatagramSocket = new DatagramSocket()
        try
        {
            // Send the WOL packet
            val ipAddress: InetAddress = InetAddress.getByName( broadcastAddress )
            val pkt:DatagramPacket = new DatagramPacket( wolPacket, 0, wolPacket.length, ipAddress, port )
            socket.send( pkt )
        }
        catch
        {
            case ioe:IOException => return false
        }
        finally
        {
            if( !socket.isClosed() )
                socket.close()
        }
        return true;
    }

    def saveMacAddress( machine: PhysicalInstance ) : Boolean = {
        val mac:String = getMacAddress(machine);
        if( mac.length > 0 ) {
            machine.mac = mac;
            println( "MAC discovered" )
            machine.tagMachine( "mac-discovered" );
            return true
        }
        return false
    }

    def machineSleep( machine: PhysicalInstance ) : Boolean = {
        // See if the machine is tagged as sleeping - if not then
        // ssh in and put it to sleep
        // If there's a daemon running send it a sleep packet

        // Execute a command on the remote machine with a timeout - since we're
        // putting the machine to sleep the command won't "complete" in the usual way
        // so we need to hang up
        val result: ExecuteResponse = machine.executeCommand( "echo -n \"mem\" > /sys/power/state", 200 )
        if( result.status.get != 0 )
            return false
        // tag the machine as sleeping
        machine.tagMachine( "sleeping" )
        true
    }

    def machineWake( machine: PhysicalInstance ) : Boolean = {
        return machineWake( machine, cluster.DEFAULT_NUM_PING_PACKETS )
    }

    def machineWake( machine: PhysicalInstance, packets: Long ): Boolean = {
        // See if the machine is tagged as sleeping - if it is then
        // send it a wakeOnLan packet
        // Make sure it is alive
        // Remove the sleeping tag
        // Send the WOL packet on port 0, 7 or 9
        val port: Int = cluster.DEFAULT_WOL_PORT
        if( !sendWolPacket( machine.broadcastAddress, machine.mac, port ) ) {
            println( "Error sending WOL packet to: " + machine.broadcastAddress + " MAC: " + machine.mac + " PORT: " + port )
            return false
        }
        // Ping machine
        if( !machinePing( machine.hostname, packets ) )
            return false
        // Remove the sleeping tag from the machine
        machine.removeTag( "sleeping" )
        return true
    }

    def machinePing( machine: PhysicalInstance ) : Boolean = {
        return machinePing( machine.hostname, cluster.DEFAULT_NUM_PING_PACKETS )
    }

    // Try to contact a machine, wait on the response, if the ping works
    // return true, else return false
    def machinePing( hostname: String, pingPackets: Long ) : Boolean = {
        // get the hostname and run the cmd "ping hostname"
        // Look for a 100% received OR for the absence of "0 received""
        var pingSuccessMsg:String = pingPackets + " received"
        var cmd:String = "/bin/ping -c " + pingPackets + " -q " +  hostname
        var result:ExecuteResponse = executeLocalCommand( cmd );
        if( result.stdout.contains(pingSuccessMsg))
            return true
        else false
    }

    def isSleeping( machine:PhysicalInstance ) : Boolean = {
        machine.isTagged( "sleeping" ) && !machinePing( machine.hostname, cluster.DEFAULT_NUM_PING_PACKETS )
    }
}

class PhysicalInstance(val hostname: String, val username: String, val privateKey: File) extends RemoteMachine
{
  val rootDirectory: File = new File("/mnt/")
  val runitBinaryPath:File = new File("/usr/bin")
  val javaCmd: File = new File("/usr/bin/java")
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
        //val mac:String = "00:1c:c0:c2:7a:85"
        
        // List all the nodes we have in the loCal cluster
        val a9 = ( "192.168.0.21", "root", new File("/home/user/.ssh/atom_key") ) // 00:1c:c0:c2:7a:85
        //val a14 = ( "192.168.0.27", "root", new File("/home/user/.ssh/atom_key") )
        val a15 = ( "192.168.0.28", "root", new File("/home/user/.ssh/atom_key") )
        val a16 = ( "192.168.0.29", "root", new File("/home/user/.ssh/atom_key") )

        // Initialize the cluster
        //val atoms = List(a9,a14,a15,a16).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        //val atoms = List(a9,a15,a16).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        val atoms = List(a9).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        // Set the broadcast address for all these machines
        atoms.map((a) => a.broadcastAddress = "192.168.0.255" )

        for{ a <- atoms } println( a.hostname + " " + a.broadcastAddress )
        
        //val atoms = List(a9).map((n) => new PhysicalInstance(n._1, n._2, n._3) );
        var atom9:PhysicalInstance = atoms{0};
        //var atom14:PhysicalInstance = atoms{1};
        var atom15:PhysicalInstance = atoms{2};
        var atom16:PhysicalInstance = atoms{3};

        // ARP miss for machine outside LAN
        val mac1:String = cluster.getMacAddress( "169.229.131.81" )
        println( "MAC1: " + mac1 )
        // ARP hit in LAN
        val mac2:String = cluster.getMacAddress( "192.168.1.1" )
        println( "MAC2: " + mac2 )

        /*
        var res:Boolean = false
        res = cluster.machineSleep( atom9 )
        println( "Sleep command: " + res )
        res = cluster.machinePing( atom9 )
        println( "Ping command: " + res )
        res = cluster.machineWake( atom9 )
        println( "Wake command: " + res )
        */
       
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
