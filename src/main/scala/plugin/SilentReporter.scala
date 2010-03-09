package com.googlecode.avro

import scala.tools.nsc.reporters._

import scala.collection.mutable.HashMap
import scala.tools.nsc.Settings
import scala.tools.nsc.util.Position

class SilentReporter extends Reporter {
  var warningReported = false;
  var errorReported = false;
  
  def info0(pos: Position, msg: String, severity: Severity, force: Boolean) = {
    severity match {
      case INFO =>
      case WARNING => warningReported = true
      case ERROR => errorReported = true
    }
  }
}
