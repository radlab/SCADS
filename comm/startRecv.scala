import edu.berkeley.cs.scads.Receiver
import org.apache.log4j.BasicConfigurator
BasicConfigurator.configure
Receiver.receive(8080)
