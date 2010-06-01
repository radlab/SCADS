package bootstrap.liftweb

import _root_.net.liftweb.common._
import _root_.net.liftweb.util._
import _root_.net.liftweb.http._
import _root_.net.liftweb.sitemap._
import _root_.net.liftweb.sitemap.Loc._
import Helpers._

import edu.berkeley.cs.lib.DotGraph

/**
  * A class that's instantiated early and run.  It allows the application
  * to modify lift's environment
  */
class Boot {
  def boot {
    // where to search snippet
    LiftRules.addToPackages("edu.berkeley.cs")

    // Build SiteMap
    val entries = List(
      Menu("Home") / "index",
      Menu("PIQL Console") / "piql",
      Menu("Scala REPL") / "repl",
      Menu("Talks") / "talks" submenus(
        Menu("Spring 2010 RAD LAB Retreat") / "talks" / "retreat")
      )

    LiftRules.setSiteMap(SiteMap(entries:_*))
    LiftRules.dispatch.prepend(DotGraph.dispatchRules)
  }
}
