import edu.berkeley.cs.scads.avro.test._

println("starting echo receiver on port 9000")
EchoReceiver.receive(9000)

println("started! type `Sender.send(9000)' to initiate sequence")
