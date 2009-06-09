package edu.berkeley.cs.scads

import AutoKey._

object Dumper {
	def main(args: Array[String]) = {
		val keyFormat = new java.text.DecimalFormat("000000000000000")
		val hostname = args(0)
		val ns = args(1)
		
		object dp extends RemoteKeySpaceProvider {
			val host = hostname
			val port = 8000
		}
		val ks = dp.getKeySpace(ns)
		println(ks)
		
		ks.lookup(KeyRange(null, null)).foreach((pair) => {
			val node = pair._1
			val rp = pair._2
			val client = node.getClient
			
			(1 to 1024*1024).foreach((k) => {
				val key = keyFormat.format(k)
				if(rp.includes(key))
					println("key: " + key + " value: " + client.get(ns, key))
			})
		})
	}
}