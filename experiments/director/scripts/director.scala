import performance._
import edu.berkeley.cs.scads.thrift.{RangeConversion, DataPlacement}
import edu.berkeley.cs.scads.keys._
import java.util.Comparator

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

case class MetricReader {
	val host = "localhost"
	val port = 6000
	val user = "root"
	val pass = ""
	val db = "metrics"
	val interval = 20.0
	val report_prob = 0.02
	var connection:Connection = null
	
	def connectToDatabase() {
        // open connection to the database
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance()
        } catch {
			case ex: Exception => ex.printStackTrace() }

        try {
            val connectionString = "jdbc:mysql://" + host + "/?user=" + user + "&password=" + pass
            connection = DriverManager.getConnection(connectionString)

		} catch {
			case ex: SQLException => {
            	// handle any errors
	            println("can't connect to the database")
	            println("SQLException: " + ex.getMessage)
	            println("SQLState: " + ex.getSQLState)
	           	println("VendorError: " + ex.getErrorCode)
	        	}
			}

        // create database if it doesn't exist and select it
        try {
            val statement = connection.createStatement

            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + db)
            statement.executeUpdate("USE " + db)

       		} catch { case ex: SQLException => ex.printStackTrace() }

        println("have connection to database")
    }

	def getWorkload(host:String):Long = {
		if (connection == null) connectToDatabase
		val workloadSQL = "select time,value from scads,scads_metrics where scads_metrics.server=\""+host+"\" and request_type=\"ALL\" and stat=\"workload\" and scads.metric_id=scads_metrics.id order by time desc limit 10"
		try {
            val statement = connection.createStatement
			val result = statement.executeQuery(workloadSQL)
			val set = result.first // set cursor to first row
			if (!set) return 0
			( (result.getLong("value")/interval/report_prob).toLong )
       		} catch { case ex: SQLException => println("Couldn't get workload"); ex.printStackTrace();-1 }
	}
}

class DataPlacementComparator extends java.util.Comparator[DataPlacement] {
	def compare(o1: DataPlacement, o2: DataPlacement): Int = {
		o1.rset.range.start_key compareTo o2.rset.range.start_key
	}
}

// boot up storage server(s) and placement server
/*val myscads = new Scads("director",xtrace_on,namespace)
myscads.init(num_servers)
val placement_host = myscads.placement.get(0).privateDnsName // assumes this script also running on ec2

// replicate or partition data responsibility
if (replicate) myscads.replicate(0,4194304)
else myscads.partition(0,4194304)

// confirm that placement has everything assigned correctly
val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
assert( dpclient.lookup_namespace(namespace).size == num_servers )
*/

case class BaselineDirector(placement_host:String, xtrace_on:Boolean, namespace: String) extends Director {
	val reader = new MetricReader // metric reader for getting storage node statistics
	val myscads = new Scads("director",xtrace_on,namespace)
	val sla = 5
	val max_workload = 100
	
	def run = {
		updateServerList
		while (running) {
			
			// obverve workload, ratio of gets and get_sets on each storage node over last interval
			server_hosts.foreach((s)=> {
				// look at each server's latency, if above threshold, start another server for split
				val load = reader.getWorkload(s)
				println("Workload for "+s+": "+load)
				if (load > max_workload) {  // SLA violation!
					myscads.addServers(1)
					val new_guy = myscads.servers.peekLast
				
					// determine current range and split-point to give new server
					val bounds = getNodeRange(s)
					val start = bounds._1
					val end = bounds._2
					val middle = (end-start)/2

					// do the move and update local list of servers
					println("Moving "+middle+" - "+end+" to "+ new_guy.privateDnsName)
					move(s,new_guy.privateDnsName,middle,end)
					updateServerList
				}
	
			})
			Thread.sleep(10000)
			
			// now look at each pair of adjacent nodes, is their combined workload low enough to merge?
			var id = 0  
			while (id <= server_hosts.size-1 && id <= server_hosts.size-2) {
				// get workload for two adjacent hosts
				val load1 = reader.getWorkload(server_hosts(id))
				val load2 = reader.getWorkload(server_hosts(id+1))
				println("Workload for "+server_hosts(id)+": "+load1)
				println("Workload for "+server_hosts(id+1)+": "+load2)
				if ( (load1 +load2) <= max_workload) {
					// do move, remove, and update director's list of servers
					val bounds = getNodeRange(server_hosts(id))
					val start = bounds._1
					val end = bounds._2
					println("Copying "+start+" - "+end+" to "+ server_hosts(id+1))
					copy(server_hosts(id),server_hosts(id+1),start,end) // do copy instead of move to avoid sync problems?
					println("Removing from placement: "+ server_hosts(id))
					val removing = server_hosts(id)
					remove(removing)
					myscads.shutdownServer(removing)
					updateServerList
				}
				id+=1
			}
			
			Thread.sleep(5000)
		}
	}
}

abstract class Director extends Runnable with RangeConversion with AutoKey {
	var running = true
	def placement_host:String
	def xtrace_on:Boolean
	def namespace:String
	def sla:Int // latency ms
	def max_workload:Int // workload one storage server can sustain to be within sla
	var server_hosts = new scala.collection.mutable.ListBuffer[String]
	
	protected def updateServerList = {
		server_hosts.clear
		val dp = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val placements = dp.lookup_namespace(namespace)
		java.util.Collections.sort(placements,new DataPlacementComparator)
		val iter = placements.iterator
		while (iter.hasNext) {
			server_hosts += iter.next.node
		}
	}

	def run
	
	protected def getNodeRange(host:String):(Int, Int) = {	
		val dp = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val s_info = dp.lookup_node(namespace,host,Scads.server_port,Scads.server_sync)
		val range = s_info.rset.range
		(Scads.getNumericKey( StringKey.deserialize_toString(range.start_key,new java.text.ParsePosition(0)) ),
		Scads.getNumericKey( StringKey.deserialize_toString(range.end_key,new java.text.ParsePosition(0)) ))
	}
	
	protected def move(source_host:String, target_host:String,startkey:Int, endkey:Int) = {
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(startkey)), new StringKey(Scads.keyFormat.format(endkey)) )
		dpclient.move(namespace,range, source_host, Scads.server_port,Scads.server_sync, target_host, Scads.server_port,Scads.server_sync)
	}
	
	protected def copy(source_host:String, target_host:String,startkey:Int, endkey:Int) = {
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(startkey)), new StringKey(Scads.keyFormat.format(endkey)) )
		dpclient.copy(namespace,range, source_host, Scads.server_port,Scads.server_sync, target_host, Scads.server_port,Scads.server_sync)
		
	}
	protected def remove(host:String) = {
		val dpclient = Scads.getDataPlacementHandle(placement_host,xtrace_on)
		val bounds = getNodeRange(host)
		val range = new KeyRange(new StringKey(Scads.keyFormat.format(bounds._1)), new StringKey(Scads.keyFormat.format(bounds._2)) )
		val list = new java.util.LinkedList[DataPlacement]()
		list.add(new DataPlacement(host,Scads.server_port,Scads.server_sync,range))
		dpclient.remove(namespace,list)
	}
	def stop = { running = false }
}
