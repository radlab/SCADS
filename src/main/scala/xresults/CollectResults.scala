package deploylib.xresults

object CollectResults {
	def main(args: Array[String]): Unit = {
		val complete = new deploylib.xresults.XmlCollection("xmldb:exist://scm.millennium.berkeley.edu:8080/exist/xmlrpc/db/complete", "experiment", "scads")
		val exps = XResult.execQuery("for $e in /experiment return <FullExperiment>{$e/@experimentId}{/*[@experimentId = $e/@experimentId]}</FullExperiment>")

		exps.foreach(e => {println(e); complete.storeUnrelatedXml(e)})
	}
}
