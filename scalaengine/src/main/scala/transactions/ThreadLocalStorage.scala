package edu.berkeley.cs.scads.storage

import scala.util.DynamicVariable

import net.lag.logging.Logger

object ThreadLocalStorage {
  val updateList = new DynamicVariable[Option[UpdateList]](None)
}

