package edu.berkeley.cs.scads.storage

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, IndexedRecord }
import org.apache.zookeeper.{ CreateMode, WatchedEvent }
import org.apache.zookeeper.Watcher.Event.EventType

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.avro.marker.AvroPair

private[storage] object IndexManager {
  // must put at least 1 field in the dummy value schema otherwise writes will
  // never work
  lazy val indexValueSchema = {
    val schema = Schema.createRecord("DummyValue", "", "", false)
    schema.setFields(Seq(new Schema.Field("b", Schema.create(Schema.Type.BOOLEAN), "", null)))
    schema
  }

  // should really be a def since dummyIndexValue is mutable, but since
  // IndexManager is private[storage] we'll just trust ourselves not to
  lazy val dummyIndexValue = {
    val rec = new GenericData.Record(indexValueSchema)
    rec.put(0, false)
    rec
  }
}

/** An IndexManager is intended to provide index maintainence for AvroPair
 * namespaces. the methods below are not intended to be thread-safe */
trait IndexManager[PairType <: AvroPair] {
  this: Namespace[_, _, _, _] =>

  import IndexManager._
  
  protected var indexCatalogue: ZooKeeperProxy#ZooKeeperNode = _ 

  onLoad {
    indexCatalogue = nsRoot("indexes")
    updateCache()
  }

  onCreate {
    ranges => {
      indexCatalogue = nsRoot.createChild("indexes", Array.empty, CreateMode.PERSISTENT)
      updateCache()
    }
  }

  private def updateCache(): Unit = {
    val callback = (evt: WatchedEvent) => evt.getType match {
      case EventType.NodeChildrenChanged => updateCache()
      case _ => /* do nothing */
    }
    updateIndexNamespaceCache(indexCatalogue.watchChildren(callback))
  }

  // TODO: this is hacky for now, until we figure out if we actually want to
  // store index namespace root nodes inside of a namespace node, or store
  // them globally with a prefix identifier. Since the storage handlers are
  // hardcoded with the assumption of a single global namespace, the actual
  // name of an index's namespace will be prefixed with the name of this
  // namespace followed by an underscroll

  protected def fromGlobalName(fullName: String): String = {
    assert(fullName.startsWith(namespace + "_"))
    fullName.substring((namespace + "_").length)
  }

  protected def toGlobalName(indexName: String): String = {
    assert(!indexName.startsWith(namespace + "_"))
    "%s_%s".format(namespace, indexName)
  }

  /** In memory cache of (index namespaces, seq of field values (left is from
   * key, right is from value)). is a volatile immutable hash map so we don't
   * have to do any synchronization when reading */
  @volatile protected var indexNamespacesCache = new HashMap[String, (IndexNamespace, Seq[Either[Int, Int]])]

  protected def updateIndexNamespaceCache(indexNodes: Seq[ZooKeeperProxy#ZooKeeperNode]): Unit = {
    // update the cache. 
    // 1) namespaces in the cache which are not in indexCatalogue.children need to
    // be deleted from the cache
    val indexCatalogueChildrenSet = indexNodes.map(_.name).toSet
    indexNamespacesCache = indexNamespacesCache filter { case (k, _) => indexCatalogueChildrenSet.contains(k) } 

    // 2) elements which are in children but not in the cache need to be added
    // into the cache
    indexNamespacesCache ++= indexNodes.filterNot(n => indexNamespacesCache.contains(n.name)).map(n => {
      // TODO: is this functionality already located somewhere, to create a
      // generic namespace w/o passing in the key/value schemas
      val ks = Schema.parse(new String(root("%s/keySchema".format(toGlobalName(n.name))).data))
      val ns = new IndexNamespace(toGlobalName(n.name), 5000, root, ks)(cluster)
      ns.load()
      (n.name, (ns, generateFieldMapping(ks)))
    })
  }

  protected def indexCache = indexNamespacesCache
  
  /** Get a list of indexes for this namespace */
  def listIndexes: Map[String, IndexNamespace] = {
    updateCache()
    indexNamespacesCache.map { case (k, v) => (k, v._1) } toMap
  }

  /** Create a new index over this pair type, with the given name. The fields
   * given MUST be (1) unique and (2) part of the key schema or the value
   * schema. The ordering of fields determines the layout of the index schema.
   * The actual schema for the index is constructed by taking fields and
   * concatenating (key schema - fields) in order to ensure uniqueness (that
   * each index can uniquely identity a pair object). Additionally, the name
   * must also be unique
   */
  def createIndex(name: String, fieldNames: Seq[String]): IndexNamespace = {
    // duplicate name check
    val fieldNameSet = fieldNames.toSet
    if (fieldNameSet.size != fieldNames.size)
      throw new IllegalArgumentException("Duplicate field names found")

    // valid field name check
    val prefixFields = fieldNames.map(name => {
      val (l, r) = (keySchema.getField(name), valueSchema.getField(name))
      if (l ne null) l
      else if (r ne null) r
      else throw new IllegalArgumentException("Invalid field name: " + name)
    })

    // construct the index key schema
    val suffixFields = keySchema.getFields.toSeq.filterNot(f => fieldNameSet.contains(f.name))
    val fields = (prefixFields ++ suffixFields).map(f => new Schema.Field(f.name, f.schema, f.doc, f.defaultValue, f.order)) // need to make clones b/c you cannot reuse field objects when constructing schemas
    val indexKeySchema = Schema.createRecord(name + "Key", "", "", false)
    indexKeySchema.setFields(fields)
    
    val indexNs = new IndexNamespace(toGlobalName(name), 5000, root, indexKeySchema)(cluster)
    // create the actual namespace with no partition strategy here 
    indexNs.create(List((None, cluster.getRandomServers(defaultReplicationFactor))))
    indexNamespacesCache += ((name, (indexNs, generateFieldMapping(indexKeySchema))))

    // add index catalogue entry- causes other clients to be notified if they
    // are watching
    indexCatalogue.createChild(name, Array.empty, CreateMode.PERSISTENT)

    // check if data exists already. if so, warn that indexes will NOT be
    // created for existing records
    if (!getRange(None, None, limit=Some(1)).isEmpty)
      logger.warning("WARNING: Indexes will not be created for previous existing records!")

    indexNs  
  }

  protected def generateFieldMapping(indexKeySchema: Schema): Seq[Either[Int, Int]] = {
    indexKeySchema.getFields.map(f => {
      val (l, r) = (keySchema.getField(f.name), valueSchema.getField(f.name))
      if (l ne null) Left(l.pos)
      else if (r ne null) Right(r.pos)
      else throw new IllegalArgumentException("invalid key schema given: " + indexKeySchema)
    }).toSeq
  }

  protected def makeIndexFor(pair: PairType, indexKeySchema: Schema, mapping: Seq[Either[Int, Int]]): IndexedRecord = {
    val rec = new GenericData.Record(indexKeySchema)
    mapping.map(m => m match {
      case Left(keyPos) => pair.key.get(keyPos)
      case Right(valuePos) => pair.value.get(valuePos)
    }).zipWithIndex.foreach { case (elem, idx) => rec.put(idx, elem) }
    rec
  }

  protected def makeIndexFor(key: IndexedRecord, value: IndexedRecord, indexKeySchema: Schema, mapping: Seq[Either[Int, Int]]): IndexedRecord = {
    val rec = new GenericData.Record(indexKeySchema)
    mapping.map(m => m match {
      case Left(keyPos) => key.get(keyPos)
      case Right(valuePos) => value.get(valuePos)
    }).zipWithIndex.foreach { case (elem, idx) => rec.put(idx, elem) }
    rec
  }

  /** Delete a pre-existing index. The index named must already exist */
  def deleteIndex(name: String): Unit = {
    listIndexes.get(name).getOrElse(throw new RuntimeException("No such index: " + name)).delete()
    indexNamespacesCache -= name
    indexCatalogue(name).delete()
  }

}
