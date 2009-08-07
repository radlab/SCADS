package performance

import deploylib._

class ScadsServerManager(deploy_name:String, xtrace_on:Boolean, namespace:String) {
	val myscads = new Scads(deploy_name,xtrace_on,namespace)
	myscads.loadState
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
		if (standbys.size >= num) {
			(1 to num).foreach((id)=>{
				ret += standbys.remove(0)
			})
		}
		ret.toList
	}
	def releaseServer(host:String):Int = {
		standbys += host
		standbys.size
	}
}