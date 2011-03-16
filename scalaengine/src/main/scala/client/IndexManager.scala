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
  val indexValueSchema = {
    val schema = Schema.createRecord("DummyValue", "", "", false)
    schema.setFields(Seq(new Schema.Field("b", Schema.create(Schema.Type.BOOLEAN), "", null)))
    schema
  }

  val dummyIndexValue = {
    val rec = new GenericData.Record(indexValueSchema)
    rec.put(0, false)
    rec
  }

  val valueReaderWriter = new AvroGenericReaderWriter[IndexedRecord](None, indexValueSchema)
  val dummyIndexValueBytes = valueReaderWriter.serialize(dummyIndexValue)
}

class IndexNamespace(
    val name: String,
    val cluster: ScadsCluster,
    val root: ZooKeeperProxy#ZooKeeperNode,
    val keySchema: Schema)
  extends Namespace 
  with SimpleRecordMetadata 
  with ZooKeeperGlobalMetadata
  with QuorumRangeProtocol
  with DefaultKeyRangeRoutable
  with RecordStore[IndexedRecord]
{ 
  import IndexManager._

  private lazy val keyReaderWriter = new AvroGenericReaderWriter[IndexedRecord](Some(remoteKeySchema), keySchema)
  
  override def bytesToKey(bytes: Array[Byte]): IndexedRecord = 
    keyReaderWriter.deserialize(bytes)
  
  override def bytesToValue(bytes: Array[Byte]): IndexedRecord =
    dummyIndexValue
  
  override def bytesToBulk(key: Array[Byte], value: Array[Byte]) =
    keyReaderWriter.deserialize(key)
  
  override def keyToBytes(key: IndexedRecord): Array[Byte] =
    keyReaderWriter.serialize(key)

  override def valueToBytes(value: IndexedRecord): Array[Byte] = 
    dummyIndexValueBytes

  override def bulkToBytes(b: IndexedRecord): (Array[Byte], Array[Byte]) = 
    (keyToBytes(b), dummyIndexValueBytes)

  override def newKeyInstance = keyReaderWriter.newInstance

  override def newRecordInstance(schema: Schema) =
    keyReaderWriter.newRecordInstance(schema)

  override lazy val valueSchema = indexValueSchema

  def asyncGetRecord(key: IndexedRecord) = asyncGet(key)
  def getRecord(key: IndexedRecord) = get(key)
}

/** An IndexManager is intended to provide index maintainence for AvroPair
 * namespaces. the methods below are not intended to be thread-safe */
trait IndexManager[BulkType <: AvroPair] extends Namespace 
	  with Protocol
	  with RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType]
	  with Serializer[IndexedRecord, IndexedRecord, BulkType]
	  with ZooKeeperGlobalMetadata {
  import IndexManager._
  
  protected var indexCatalogue: ZooKeeperProxy#ZooKeeperNode = _
  logger.info("IndexManger Constructor: %s", namespace)

  onOpen {isNew =>
    logger.debug("Opening index catalog for namespace %s", namespace)
    indexCatalogue = nsRoot.getOrCreate("indexes")
    updateCache()
    isNew
  }

  onCreate {
    logger.debug("Creating index catalog for namespace %s", namespace)
    indexCatalogue = nsRoot.createChild("indexes", Array.empty, CreateMode.PERSISTENT)
    updateCache()
  }

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    val optOldValue = get(key)
    indexCache.values.foreach { case (ns, mapping) =>
      val optOldIndex = optOldValue.map(oldValue => makeIndexFor(key, oldValue, ns.keySchema, mapping))
      value match {
        case None => // delete old index (if it exists)
          optOldIndex.foreach(oldIndex => ns.put(oldIndex, None))
        case Some(newValue) => // update index if necessary, deleting stale ones if necessary
          val newIndex = makeIndexFor(key, newValue, ns.keySchema, mapping)
          val optStaleValue = optOldIndex.flatMap(oldIndex => if (oldIndex != newIndex) Some(oldIndex) else None)
          optStaleValue.foreach(staleValue => ns.put(staleValue, None))
          if (optOldValue.isEmpty || optStaleValue.isDefined)
            ns.put(newIndex, Some(dummyIndexValue))
      }
    }
    // put the actual key/value pair AFTER index maintainence
    super.put(key, value)
  }

  override def ++=(that: TraversableOnce[BulkType]): Unit = {
    logger.info("Putting index entries")
    that.foreach { pair => {
      putBulkBytes(keyToBytes(pair.key), valueToBytes(pair.value))

      indexCache.values.map { case (ns, mapping) =>
	ns.putBulkBytes(ns.keyToBytes(makeIndexFor(pair.key, pair.value, ns.keySchema, mapping)), dummyIndexValueBytes)
      }.toList
    }}

    flushBulkBytes
    indexCache.values.foreach(_._1.flushBulkBytes)
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
      val ns = new IndexNamespace(toGlobalName(n.name), cluster, root, ks)
      ns.open()
      (n.name, (ns, generateFieldMapping(ks)))
    })
  }

  protected def indexCache = indexNamespacesCache
  
  /** Get a list of indexes for this namespace */
  def listIndexes: Map[String, IndexNamespace] = {
    updateCache()
    indexNamespacesCache.map { case (k, v) => (k, v._1) } toMap
  }

  /** getOrCreate a secondary index over the given fields */
  def getOrCreateIndex(fieldNames: Seq[String]): IndexNamespace = {
    val idxName = fieldNames.mkString("(", ",", ")")

    listIndexes.get(idxName) match {
      case Some(idx) => idx
      case None => createIndex(idxName, fieldNames)
    }
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
    
    val indexNs = new IndexNamespace(toGlobalName(name), cluster, root, indexKeySchema)
    // create the actual namespace with no partition strategy here 
    indexNs.create
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
