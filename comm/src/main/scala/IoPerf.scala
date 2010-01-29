import java.net._
import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.util._
import edu.berkeley.cs.scads.Record


object IoPerfSender {
def main(args: Array[String]): Unit = {
val testSize = 1000000
(1 to 10).foreach(t => {
val sock = new Socket(args(0), 9000)
val enc = new BinaryEncoder(sock.getOutputStream)
val writer = new SpecificDatumWriter(classOf[Record])
val start = System.currentTimeMillis()
(1 to testSize).foreach(i => {
val r = new Record
r.key = new Utf8("testKey")
r.value = new Utf8("testValue")
writer.write(r, enc)
})
enc.flush
val end = System.currentTimeMillis()
println((testSize.toFloat / ((end - start)/1000.0)) + "req/sec")
sock.close
})
}
}

object IoPerfReceiver {
def main(args: Array[String]): Unit = {
val server = new ServerSocket(9000)
while(true) {
val sock = server.accept
val buff = new Array[Byte](8192)
val iStream = sock.getInputStream()

var read = iStream.read(buff)
var total = 0
while(read > 0) {
total += read
read = iStream.read(buff)
}
println("read " + total)
}
}
}
