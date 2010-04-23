package deploylib.xresults

import deploylib._
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
	lazy val updateService = collection.getService("XUpdateQueryService", "1.0").asInstanceOf[XUpdateQueryService]

	def storeUnrelatedXml(elem: Elem): Unit = storeUnrelatedXml(null, elem)
	def storeUnrelatedXml(name: String, elem: Elem): Unit = {
		Util.retry(10) {
			val doc = collection.createResource(name, "XMLResource").asInstanceOf[XMLResource]
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

	def moveFragment(srcQuery: String, destQuery: String): Unit = {
		val elem = execQuery(srcQuery).next
		updateService.update(
			<xupdate:modifications version="1.0" xmlns:xupdate="http://www.xmldb.org/xupdate">
				<xupdate:remove select={srcQuery}/>
			</xupdate:modifications>.toString
		)
		updateService.update(
			<xupdate:modifications version="1.0" xmlns:xupdate="http://www.xmldb.org/xupdate">
				<xupdate:append select={destQuery}>
					<xupdate:element name={elem.label}>
						{elem.attributes.elements.map(a => <xupdate:attribute name={a.key}>{a.value}</xupdate:attribute>)}
						{elem.child}
					</xupdate:element>
				</xupdate:append>
			</xupdate:modifications>.toString
		)
	}
}
