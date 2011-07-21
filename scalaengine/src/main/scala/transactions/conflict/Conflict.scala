package transactions.conflict

import actors.threadpool.ThreadPoolExecutor.AbortPolicy

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: 7/20/11
 * Time: 2:00 PM
 * To change this template use File | Settings | File Templates.
 */


//Thread-safe

sealed trait Update {
  val key: String
}

object Update {
  def unapply(getOp: Update): Option[String] = {
    Some(getOp.key)
  }
}

case class LogUpdate(key : String, val operation: String, val delta: String) extends Update

case class PhysUpdate(key : String, val oldValue: String, val newValue: String) extends Update

case class VersUpdate(key : String, val read_vers: String, val newValue: String) extends Update

case class CommandSequence()

object Status extends Enumeration {
  type Status = Value
  val Commit, Abort, Unknown = Value
}


abstract class PendingUpdate {


  //Even at an abort store the trxID and always abort it afterwards   --> Check NoOp property
  //Is only allowed to accept, if the operation will be successful, even if all outstanding Cmd might be NullOps
  def acceptCommand(update: Update, trxID: Int): Boolean

  def commit(trxID: Int) //Value is chosen and confirms trx state

  def abort(trxID: Int) //Changes

  def getDecision(trxID: Int): Status.Status

  def getCmdSeq(): CommandSequence

}

abstract class IntegrityConstrainChecker {

  def check(key: String, newValue: String): Boolean

}

abstract class ConflictResolver {

  def getLUB(sequences: Array[CommandSequence]): CommandSequence

  def getGLB(sequences: Array[CommandSequence]): CommandSequence

}