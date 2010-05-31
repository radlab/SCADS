package com.googlecode.avro
package plugin

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
        println("[INFO] " + msg)
      case WARNING => 
        warningReported = true
        println("[WARNING] " + msg) 
      case ERROR => 
        errorReported = true
        println("[ERROR] " + msg)
    }
    severity.count += 1
  }
}
