import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.Record
import org.apache.avro.util.Utf8

def newRec() = {val x = new Record; x.key = new Utf8("testKey"); x.value = new Utf8("testValue"); x}

val c = new SampleChannelManager
