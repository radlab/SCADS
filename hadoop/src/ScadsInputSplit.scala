import java.io.IOException;
import java.util._;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;

class ScadsInputSplit extends InputSplit {
	val host = "localhost"
	val port = 9091
	val ns = "greptest"

	val con = new java.net.Socket(host, port)
	val out = con.getOutputStream()
	val in = con.getInputStream()
	out.write(2)
	//FIX ME: actually send a fourbyte integer
	out.write(ns.length)
	out.write(0)
	out.write(0)
	out.write(0)

	out.write(ns.getBytes())
	out.flush()

	val versionString = new Array[Byte](11)
	in.read(versionString)

	assert("SCADSBDB0.1" equals new String(versionString))

	println("new connection made")



	def getLength(): Long = 0
	def getLocations(): Array[String] = Array(host)
	def readFields(in: java.io.DataInput){}
	def write(out: java.io.DataOutput) {}

	def readNext(): String ={
		val lenBytes = new Array[Byte](4)
		con.getInputStream.read(lenBytes)
		//val len = lenBytes.foldLeft(0)((a: Int,b: Byte) => b + (a << 8))
		val len = lenBytes.foldRight(0)((a:Byte, b:Int) => (b << 8) + a)

		val data = new Array[Byte](len)
		con.getInputStream.read(data)
		println("read: " + new String(data))
		new String(data)
	}
}
