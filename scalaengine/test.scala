import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8

settings.maxPrintString = 100000

TestScalaEngine
val k1 = new IntRec
k1.f1 = 1

TestScalaEngine.cluster.createNamespace("test1", k1.getSchema(), k1.getSchema())

val cr = new ConfigureRequest
cr.namespace = "test1"
cr.partition = "1"
Sync.makeRequest(TestScalaEngine.node, new Utf8("Storage"), cr)

val pr = new PutRequest
pr.namespace = "test1"
pr.key = k1.toBytes
pr.value = "Hello World"
println(pr)
Sync.makeRequest(TestScalaEngine.node, new Utf8("Storage"), pr)

val gr = new GetRequest
gr.namespace = "test1"
gr.key = k1.toBytes
println(gr)
Sync.makeRequest(TestScalaEngine.node, new Utf8("Storage"), gr)
