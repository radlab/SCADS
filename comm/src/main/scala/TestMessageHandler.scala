package edu.berkeley.cs.scads.comm

import collection.immutable.HashSet
import actors.Actor

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Aug 31, 2010
 * Time: 5:28:13 PM
 * To change this template use File | Settings | File Templates.
 */

class TestMessageHandler extends MessageHandler {
  private var blockedSender = new HashSet[ActorId]
  private var blockedReceiver = new HashSet[ActorId]

  def blockSenders(ra : List[RemoteActorProxy]) = ra.foreach(blockSender(_))

  def blockSender(ra : RemoteActorProxy) = {
    //println("Blocked sender " + ra.id)
    blockedSender  += ra.id
  }

  def blockReceivers(ra : List[RemoteActorProxy]) = ra.foreach(blockReceiver(_))

  def blockReceiver(ra : RemoteActorProxy) = {
    //println("Blocked receiver " + ra.id)
    blockedReceiver  += ra.id
  }

  def unblockSenders(ra : List[RemoteActorProxy]) = ra.foreach(unblockSender(_))

  def unblockSender(ra : RemoteActorProxy) = {
    //println("Unblocked sender " + ra.id)
    blockedSender  -= ra.id
  }

  def unblockReceivers(ra : List[RemoteActorProxy]) = ra.foreach(unblockReceiver(_))
  def unblockReceiver(ra : RemoteActorProxy) = {
    //println("Unblocked receiver " + ra.id)
    blockedReceiver  -= ra.id
  }

  override def sendMessage(dest: RemoteNode, msg: Message) : Unit = {

    if(msg.src.isDefined && blockedSender.contains(msg.src.get)) {
      //println("Dropped send msg" + msg)
      return
    }
    if(blockedReceiver.contains(msg.dest)){
      //println("Dropped send msg" + msg)
      return
    }
    //println("Sending message from " + msg.src.getOrElse("") + "to" + msg.dest)
    super.sendMessage(dest, msg)
  }

  override def sendMessageBulk(dest: RemoteNode, msg: Message) : Unit = {

    if(msg.src.isDefined && blockedSender.contains(msg.src.get)){
      //println("Dropped send msg" + msg)
      return
    }
    if(blockedReceiver.contains(msg.dest)){
      //println("Dropped send msg" + msg)
      return
    }
    //println("Sending bulk message from " + msg.src.getOrElse("") + "to" + msg.dest)
    super.sendMessageBulk(dest, msg)
  }


  override def receiveMessage(src: RemoteNode, msg: Message) : Unit =  {

    if(blockedReceiver.contains(msg.dest)){
      //println("Dropped received msg" + msg)
      return
    }
     //println("Receive message to " + msg.dest)
    super.receiveMessage(src, msg)
   }


  override def registerActor(a: Actor): RemoteActorProxy = {
    //println("registered actor " + a)
    super.registerActor(a)
  }

  override def unregisterActor(ra: RemoteActorProxy) {
    //println("unregistered actor " + ra)
    super.unregisterActor(ra)
  }

  override def registerService(service: MessageReceiver): RemoteActor = {

    val id = super.registerService(service)
    //println("registered service " + id + " service:" + service.getClass.getName)
    return id
  }

  override def registerService(id: String, service: MessageReceiver): RemoteActorProxy = {
    //println("registered service " + id + ":" + service)
    super.registerService(id, service)
  }


}