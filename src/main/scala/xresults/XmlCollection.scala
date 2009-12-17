package deploylib.xresults

import org.xmldb.api.base._
import org.xmldb.api.modules._
import org.xmldb.api._
import org.exist.xmldb.DatabaseInstanceManager

import org.apache.log4j.Logger
import scala.xml.{NodeSeq, UnprefixedAttribute, Elem, Node, Null, Text, TopScope }

class XmlCollection(url: String, username: String, password: String) {
  val logger = Logger.getLogger("deploylib.xresult")

  lazy val collection = getCollection()
	lazy val queryService = collection.getService("XPathQueryService", "1.0").asInstanceOf[XPathQueryService]

	def storeUnrelatedXml(elem: Elem): Unit = {
		Util.retry(10) {
			val doc = collection.createResource(null, "XMLResource").asInstanceOf[XMLResource]
			doc.setContent(elem)
			logger.debug("Storing result: " + doc.getId)
			collection.storeResource(doc)
		}
	}

	def execQuery(query: String): Iterator[Elem] = {
		class ResultIterator(ri: ResourceIterator) extends Iterator[Elem] {
			def hasNext: Boolean = ri.hasMoreResources()
			def next: Elem = scala.xml.XML.loadString(ri.nextResource().getContent().asInstanceOf[String])
		}
		new ResultIterator(queryService.query(query).getIterator)
	}

	protected def getCollection():Collection = {
		val cl = Class.forName("org.exist.xmldb.DatabaseImpl")
		val database = cl.newInstance().asInstanceOf[Database]
		database.setProperty("create-database", "true")
		DatabaseManager.registerDatabase(database)
		DatabaseManager.getCollection(url, username, password)
	}
}
