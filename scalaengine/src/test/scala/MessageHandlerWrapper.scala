package edu.berkeley.cs.scads.test

import collection.immutable.HashSet
import edu.berkeley.cs.scads.comm._

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Aug 31, 2010
 * Time: 5:28:13 PM
 * To change this template use File | Settings | File Templates.
 */

class MessageHandlerWrapper extends MessageHandler {
  private var blockedSender = new HashSet[ActorId]
  private var blockedReceiver = new HashSet[ActorId]

  def blockSender(ra : RemoteActorProxy) = {
    blockedSender  += ra.id
  }

  def blockReceiver(ra : RemoteActorProxy) = {
    blockedReceiver  += ra.id
  }

  def unblockSender(ra : RemoteActorProxy) = {
    blockedSender  -= ra.id
  }

  def unblockReceiver(ra : RemoteActorProxy) = {
    blockedReceiver  -= ra.id
  }

  override def sendMessage(dest: RemoteNode, msg: Message) : Unit = {
    if(msg.src.isDefined && blockedSender.contains(msg.src.get)) {
      println("Dropped send msg" + msg)
      return
    }
    if(blockedReceiver.contains(msg.dest)){
      println("Dropped send msg" + msg)
      return
    }
    super.sendMessage(dest, msg)
  }

  override def sendMessageBulk(dest: RemoteNode, msg: Message) : Unit = {
    if(msg.src.isDefined && blockedSender.contains(msg.src.get)){
      println("Dropped send msg" + msg)
      return
    }
    if(blockedReceiver.contains(msg.dest)){
      println("Dropped send msg" + msg)
      return
    }
    super.sendMessageBulk(dest, msg)
  }


  override def receiveMessage(src: RemoteNode, msg: Message) : Unit =  {
    if(blockedReceiver.contains(msg.dest)){
      println("Dropped received msg" + msg)
      return
    }
    super.receiveMessage(src, msg)
   }



}