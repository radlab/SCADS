import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TBinaryProtocol, XtBinaryProtocol}

import org.apache.log4j.BasicConfigurator

case class RemoteDataPlacement(host: String, port: Int, logger: Logger) extends RemoteDataPlacementProvider


        val logger = Logger.getLogger("deploylib.remoteMachine")
        logger.setLevel(Level.INFO)

        BasicConfigurator.configure()

        val storageNodes = Array(r15,r11,r6,r13)
        val dataPlacementNode = r10

        (storageNodes ++ Array(dataPlacementNode)).foreach(_.services.foreach(_.stop))

        storageNodes.foreach((n)=> {
            n.setupRunit
            val storageNodeService = new JavaService(
                    "../scalaengine/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.storage.JavaEngine","")
            storageNodeService.action(n)
            n.services.foreach((s) => {
                    println(s)
                    s.watchLog
                    s.start
                    println(s.status)
            })
        })

        val dataPlacementNodeService = new JavaService(
                "../placement/placement-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.placement.SimpleDataPlacementApp","")
        dataPlacementNode.setupRunit
        dataPlacementNodeService.action(dataPlacementNode)
        //dataPlacementNode.services.foreach((s) => {
        //            println(s)
        //            s.watchLog
        //            s.start
        //            println(s.status)
        //    })

        val rservice = dataPlacementNode.services(0)

        rservice.watchLog
        rservice.start
        while( !rservice.status.trim.equals("run") ) {
            logger.info("got status '" + rservice.status + "', expecting 'run'")
            Thread.sleep(1000);// try to mitigate busy-wait
        }

        val dpclient = getDataPlacementHandle("r10.millennium.berkeley.edu",false)
        println("-----------------GOT DPCLIENT:" + dpclient)

        val range1 = new RangeSet();
        val k1 = new StringField
        k1.value = "a"
        val k2 = new StringField
        k2.value = "g"
        range1.setStart_key(k1.serializeKey)
        range1.setEnd_key(k2.serializeKey)
        val rs1 = new RecordSet(3,range1,null,null)

        val dp1 = new DataPlacement("r15.millennium.berkeley.edu",9000,9000,rs1)
        val l1 = new java.util.LinkedList[DataPlacement]
        l1.add(dp1)
        dpclient.add("ent_user", l1)

        implicit val env = new Environment
        env.placement = new RemoteDataPlacement("r10.millennium.berkeley.edu",8000,logger)
        env.session = new TrivialSession
        env.executor = new TrivialExecutor

        logger.info("creating new user")
        val user1 = new user
        logger.info("setting user's name")
        user1.name("b")
        logger.info("saving user")
        user1.save

        val rtn = Queries.userByName("b")
        rtn.foreach(println(_))

        (storageNodes ++ Array(dataPlacementNode)).foreach(_.services.foreach(_.stop))


    def getDataPlacementHandle(h:String,xtrace_on:Boolean):KnobbedDataPlacementServer.Client = {
        val p = 8000 // default port
        var haveDPHandle = false
        var dpclient:KnobbedDataPlacementServer.Client = null
        while (!haveDPHandle) {
            try {
                val transport = new TFramedTransport(new TSocket(h, p))
                val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
                dpclient = new KnobbedDataPlacementServer.Client(protocol)
                transport.open()
                haveDPHandle = true
            } catch {
                case e: Exception => { println("don't have connection to placement server, waiting 1 second: " + e.getMessage); e.printStackTrace; Thread.sleep(1000) }
            }
        }
        dpclient
    }
