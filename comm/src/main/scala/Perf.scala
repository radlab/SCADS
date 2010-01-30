import edu.berkeley.cs.scads.comm._
import org.apache.avro.util._
import java.nio.ByteBuffer

class TrivialChannelManager extends AvroChannelManager[Record, Record] {
	def receiveMessage(src: RemoteNode, msg: Record): Unit = {
		null
	}
}

object PerfSender {
	def main(args: Array[String]): Unit = {
		val testSize = 1000000
		val mgr = new TrivialChannelManager
		val dest = RemoteNode(args(0), 9000)

		(1 to 10).foreach(t => {
			val start = System.currentTimeMillis()
			(1 to testSize).foreach(i => {
				val r = new Record
				r.key = ByteBuffer.allocate(4).put("test".getBytes)
				r.value = ByteBuffer.allocate(4).put("test".getBytes)
				mgr.sendMessage(dest, r)
			})
			val end = System.currentTimeMillis()
			println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
		})
	}
}

object PerfReceiver {
	def main(args: Array[String]): Unit = {
		val mgr = new TrivialChannelManager
		mgr.startListener(9000)
	}
}
