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
import net.lag.logging.Logger

import scala.xml.XML

import scala.tools.nsc._

object InitRepl
case class ExecuteScala(scalaCmd: String)
case class DisplayNodeSeq(seq: NodeSeq)

class Repl extends CometActor {
  val logger = Logger()
  private val settings = new Settings
  val contextJars = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList
  //HACK
  val sbtJars = deploylib.Util.readFile(new File("allJars")).split("\n").map(new File(_))

  settings.classpath.value = (sbtJars ++ contextJars).mkString(":")
  logger.info("Using classpath: %s", settings.classpath.value)
  private val stream = new ByteArrayOutputStream
  private val writer = new PrintWriter(stream)
  var interpreter: Interpreter = null

  this ! InitRepl

//TODO: this probably shouldn't be hardcoded here
val initSeq = deploylib.Util.readFile(new java.io.File("setup.scala"))

  override def lowPriority = {
    case InitRepl => {
      interpreter = new Interpreter(settings, writer)
      interpreter.bind("repl", "edu.berkeley.cs.comet.Repl", this)
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
    val callback = SHtml.jsonCall(ValById("test"), v => {println(v); Noop})

    SHtml.ajaxText("", (cmd: String) => { this ! ExecuteScala(cmd); SetValueAndFocus("cmdline", "") }, ("id", "cmdline")) ++
    <input type="text" id="test" onkeypress={callback._2}/> ++
      SHtml.a(() => { this ! InitRepl; SetHtml("history", <p>Initalizing REPL</p>) }, <span>Reset REPL</span>)
  }
}
