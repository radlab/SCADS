package edu.berkeley.cs.scads.comm

import org.apache.log4j.Logger
import org.apache.log4j.BasicConfigurator

object EchoActor extends ServiceHandler {
  implicit val remoteActor = MessageHandler.registerService(this)
  protected val logger = Logger.getLogger("scads.comm.echoactor")

  def receiveMessage(src: Option[RemoteActorProxy], msg: MessageBody): Unit = {
    logger.info("Received message: " + src + " " + msg)
    src.foreach(_ ! msg)
  }

  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure()
    logger.info("EchoActor running at: " + remoteActor)
  }
}
