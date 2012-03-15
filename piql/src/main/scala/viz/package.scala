package edu.berkeley.cs.scads.piql

import plans._

package object viz {
  implicit def graphLogical[A <: LogicalPlan](plan: A) = new {
    def x: Unit = println("test")

    def graphViz: Unit = {
      val grapher = new GraphLogicalPlan()
      grapher.generate(plan)
      val svgFile = grapher.finish
      Runtime.getRuntime.exec(Array("open", svgFile.getCanonicalPath))
    }
  }
}
