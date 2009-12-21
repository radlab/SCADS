package deploylib.xresults

import java.io.File

object XResultCSV {
	def main(args: Array[String]): Unit = {
		val query = Util.readFile(new java.io.File(args(0)))
		val results = XResult.execQuery(query).buffered
		val headings = results.head.child.filter(_.isInstanceOf[scala.xml.Elem]).map(_.label)
		println(headings.mkString("", "\t", ""))
		results.foreach(r => println(headings.map(r \ _).map(_.text).mkString("", "\t", "")))
	}
}
