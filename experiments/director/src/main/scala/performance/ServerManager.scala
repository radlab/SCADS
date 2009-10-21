package performance

import scads.deployment._
import deploylib._

class ScadsServerManager(deploy_name:String, xtrace_on:Boolean, namespace:String) {
	import edu.berkeley.cs.scads.WriteLock
	val lock = new WriteLock

	val myscads = ScadsLoader.loadState(deploy_name)
	var standbys = new scala.collection.mutable.ListBuffer[String]()

	def bootServers(num:Int): List[String] = {
		var new_guys = new scala.collection.mutable.ListBuffer[String]()
		val old_num = myscads.servers.size
		myscads.addServers(num)
		(old_num to (myscads.servers.size-1) ).foreach((id) => {
			val server = myscads.servers.get(id).privateDnsName
			new_guys += server
			standbys += server
		})
		new_guys.toList
	}
	def killServer(host:String) = myscads.shutdownServer(host)
	def getServers(num:Int): List[String] = {
		var ret = new scala.collection.mutable.ListBuffer[String]()
		lock.lock
		try {
			if (standbys.size >= num) {
				(1 to num).foreach((id)=>{
					ret += standbys.remove(0)
				})
			}
			lock.unlock
		} finally lock.unlock
		ret.toList
	}
	def releaseServer(host:String):Int = {
		lock.lock
		try {
			standbys += host
			val ret = standbys.size
			lock.unlock
			ret
		} finally lock.unlock
	}
}
