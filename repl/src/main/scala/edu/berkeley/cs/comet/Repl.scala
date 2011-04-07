package edu.berkeley.cs
package comet

import _root_.scala.actors.Actor
import _root_.scala.xml.{ Elem, NodeSeq, Text }
import _root_.net.liftweb.http.{ ListenerManager, CometActor, CometListener, SHtml }

import net.liftweb.util._
import net.liftweb.http._
import net.liftweb.http.js._
import net.liftweb.http.js.JsCmds._
import net.liftweb.http.js.JE._
import net.liftweb.http.js.jquery.JqJsCmds.AppendHtml
import java.io._
import Helpers._
import _root_.net.liftweb.common.{ Box, Empty, Full }
import net.lag.logging.Logger
import net.liftweb.util.BindPlus._


import scala.xml.XML

import scala.tools.nsc._

object InitRepl
case class ExecuteScala(scalaCmd: String)
case class DisplayNodeSeq(seq: NodeSeq)

class Repl extends CometActor {
  self =>
  val logger = Logger()
  private val settings = new Settings
  val contextJars = Thread.currentThread.getContextClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs.toList
  //HACK
  val sbtJars = deploylib.Util.readFile(new File("allJars")).split("\n").map(new File(_))

//  settings.usejavacp.value = true
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
      outputToConsole(<p>> { cmd }</p>)
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

  def render: RenderOut = 
    Script(cmdLineHandler.jsCmd) ++
    //HACK
    Script(JsRaw("""var cmdline =$('#cmdline').cmd({
      prompt: 'scala>',
      width: '100%',
      commands: function(command) {""" +
        cmdLineHandler.call("interpret", JsRaw("command")).toJsCmd +
     """}});""")
    ) ++ <br/> ++ <h2>> ScratchPad</h2> ++
    <button onClick={cmdLineHandler.call("interpret", JsRaw("document.getElementById(\"scratchpad\").value.substring(document.getElementById(\"scratchpad\").selectionStart, document.getElementById(\"scratchpad\").selectionEnd)")).toJsCmd}>execute</button> ++
    <textarea id="scratchpad" onClick="cmdline.disable()">
    </textarea>

  val cmdLineHandler = new JsonHandler {
    def apply(in : Any) : JsCmd = in match {
      case JsonCmd("interpret", _, cmd: String, _) => {
	self ! ExecuteScala(cmd)
	Noop
      }
      case x => Noop
    }
  }

  override lazy val fixedRender: Box[NodeSeq] = None
}
