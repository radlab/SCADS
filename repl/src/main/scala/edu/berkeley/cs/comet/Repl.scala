package edu.berkeley.cs
package comet

import _root_.scala.actors.Actor
import _root_.scala.xml.{ Elem, NodeSeq, Text }
import _root_.net.liftweb.http.{ ListenerManager, CometActor, CometListener, SHtml }

import _root_.net.liftweb.util.Helpers
import net.liftweb.http.SHtml
import net.liftweb.http.RenderOut
import net.liftweb.http.js.JsCmds._
import net.liftweb.http.js.JE._
import net.liftweb.http.js.jquery.JqJsCmds.AppendHtml
import java.io._
import Helpers._
import _root_.net.liftweb.common.{ Box, Empty, Full }

import scala.xml.XML

import scala.tools.nsc._

object InitRepl
case class ExecuteScala(scalaCmd: String)
case class DisplayNodeSeq(seq: NodeSeq)

class Repl extends CometActor {
  private val settings = new Settings
  settings.classpath.value = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList.mkString(":")
  private val stream = new ByteArrayOutputStream
  private val writer = new PrintWriter(stream)
  private var interpreter: Interpreter = null

  this ! InitRepl

//TODO: this probably shouldn't be hardcoded here
val initSeq = """
import deploylib._
import deploylib.mesos._
import deploylib.ec2._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._
import java.io.File
import edu.berkeley.cs.scads.perf._
val mesosExecutor = new File("../mesos/frameworks/deploylib/java_executor").getCanonicalPath
implicit val scheduler = LocalExperimentScheduler("Local Console", "1@" + java.net.InetAddress.getLocalHost.getHostAddress + ":5050", mesosExecutor)
implicit val classpath = System.getProperty("java.class.path").split(":").map(j => new File(j).getCanonicalPath).map(p => ServerSideJar(p)).toSeq
implicit val zookeeper = ZooKeeperHelper.getTestZooKeeper().root
"""

  override def lowPriority = {
    case InitRepl => {
      interpreter = new Interpreter(settings, writer)
      interpreter.bind("repl", "net.liftweb.http.CometActor", this)
      interpret("import edu.berkeley.cs.scads.lib.ReplHelpers._")
      interpret("implicit val replImplicit = repl")

      initSeq.split("\n").foreach(interpret)

      outputToConsole(<p>Ready...</p>)
    }
    case DisplayNodeSeq(seq) => outputToConsole(seq)
    case ExecuteScala(cmd) => {
      outputToConsole(<p>{ cmd }</p>)
      interpret(cmd)
    }
  }

  private def interpret(scalaCmd: String) = {
    interpreter.interpret(scalaCmd)
    flushConsole()
  }

  private def flushConsole() = {
    writer.flush()
    val result = stream.toString
    if (result.length > 0)
      outputToConsole(<span>{ NodeSeq.fromSeq(result.split("\n").map(line => { <p>{ line }</p> })) }</span>)
    stream.reset()
  }

  private def outputToConsole(text: NodeSeq): Unit = {
    partialUpdate(CmdPair(AppendHtml("history", text), JsRaw("""var objDiv = document.getElementById("history"); objDiv.scrollTop = objDiv.scrollHeight;""")))
  }

  def render: RenderOut = <span></span>

  override lazy val fixedRender: Box[NodeSeq] = {
    SHtml.ajaxText("", (cmd: String) => { this ! ExecuteScala(cmd); SetValueAndFocus("cmdline", "") }, ("id", "cmdline")) ++
      SHtml.a(() => { this ! InitRepl; SetHtml("history", <p>Initalizing REPL</p>) }, <span>Reset REPL</span>)
  }
}
