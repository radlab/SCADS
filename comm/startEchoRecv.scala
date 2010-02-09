import edu.berkeley.cs.scads.EchoReceiver
import org.apache.log4j.BasicConfigurator
BasicConfigurator.configure
EchoReceiver.receive(8080)
