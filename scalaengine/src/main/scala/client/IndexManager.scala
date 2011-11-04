package edu.berkeley.cs.scads.storage
package client
package index

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, IndexedRecord }
import org.apache.zookeeper.{ CreateMode, WatchedEvent }
import org.apache.zookeeper.Watcher.Event.EventType

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.avro.marker._
import org.apache.avro.util.Utf8

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

  private val indexDefinition = "indexDef"
}

sealed trait IndexType extends AvroUnion
case class AttributeIndex(var fieldName: String) extends AvroRecord with IndexType
case class TokenIndex(var fieldNames: Seq[String]) extends AvroRecord with IndexType

case class IndexDefinition(var fields: Seq[IndexType]) extends AvroRecord


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
  with Serializer[IndexedRecord, IndexedRecord, IndexedRecord]
  with NamespaceIterator[IndexedRecord]
  with DebuggingClient
{ 
  import IndexManager._

  val valueClass = classOf[IndexedRecord].getName

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

  override lazy val valueSchema = indexValueSchema

  def asyncGetRecord(key: IndexedRecord) = asyncGet(key)
  def getRecord(key: IndexedRecord) = get(key)

  def newRecord(schema: Schema) = keyReaderWriter.newRecord(schema)
}

/** An IndexManager is intended to provide index maintainence for AvroPair
 * namespaces. the methods below are not intended to be thread-safe */
trait IndexManager[BulkType <: AvroPair] extends Namespace 
	  with Protocol
	  with RangeKeyValueStoreLike[IndexedRecord, IndexedRecord, BulkType]
	  with Serializer[IndexedRecord, IndexedRecord, BulkType]
	  with ZooKeeperGlobalMetadata
    with RecordStore[BulkType] {
  import IndexManager._

  protected var indexCatalogue: ZooKeeperProxy#ZooKeeperNode = _
  logger.info("IndexManger Constructor: %s", namespace)

  /**
   * In memory cache of (index namespaces, seq of field values (left is from
   * key, right is from value)). is a volatile immutable hash map so we don't
   * have to do any synchronization when reading
   */
  @volatile protected var indexNamespacesCache = new HashMap[String, (IndexNamespace, IndexDefinition)]

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

  override abstract def repartition(data: Seq[BulkType], replicationFactor: Int): Unit = {
    indexCache.values.foreach {
      case (ns, mapping) =>
        ns.repartition(data.flatMap(d => makeIndexFor(d.key, d.value, ns.keySchema, mapping)), replicationFactor)
    }
    super.repartition(data, replicationFactor)
  }

  override abstract def put(key: IndexedRecord, value: Option[IndexedRecord]): Unit = {
    val optOldValue = get(key)
    indexCache.values.foreach { case (ns, mapping) =>
      val oldIndexValues = optOldValue.map(oldValue => makeIndexFor(key, oldValue, ns.keySchema, mapping)).getOrElse(Nil)

      value match {
        case None => // delete old index (if it exists)
          oldIndexValues.foreach(oldIndex => ns.put(oldIndex, None))
        case Some(newValue) => {// update index if necessary, deleting stale ones if necessary
          val newIndexValues = makeIndexFor(key, newValue, ns.keySchema, mapping)
          val toDelete = oldIndexValues diff newIndexValues
          val toAdd = newIndexValues diff oldIndexValues

          toAdd.foreach(ns.put(_, Some(dummyIndexValue)))
          toDelete.foreach(ns.put(_, None))
        }
      }
    }
    // put the actual key/value pair AFTER index maintainence
    super.put(key, value)
  }

  override def ++=(that: TraversableOnce[BulkType]): Unit = {
    logger.info("Putting index entries")
    that.foreach { pair => {
      putBulkBytes(keyToBytes(pair.key), valueToBytes(pair.value))

      indexCache.values.foreach { case (ns, mapping) =>
	      makeIndexFor(pair.key, pair.value, ns.keySchema, mapping)
          .map(ns.keyToBytes)
          .foreach(ns.putBulkBytes(_, dummyIndexValueBytes))
      }
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

  protected def newIndexNamespace(name: String,
                                  cluster: ScadsCluster,
                                  root: ZooKeeperProxy#ZooKeeperNode,
                                  keySchema: Schema): IndexNamespace = {
    new IndexNamespace(name, cluster, root, keySchema)
  }

  protected def updateIndexNamespaceCache(indexNodes: Seq[ZooKeeperProxy#ZooKeeperNode]): Unit = {
    // update the cache. 
    // 1) namespaces in the cache which are not in indexCatalogue.children need to
    // be deleted from the cache
    val indexCatalogueChildrenSet = indexNodes.map(_.name).toSet
    indexNamespacesCache = indexNamespacesCache filter { case (k, _) => indexCatalogueChildrenSet.contains(k) } 

    // 2) elements which are in children but not in the cache need to be added
    // into the cache
    indexNamespacesCache ++= indexNodes.filterNot(n => indexNamespacesCache.contains(n.name)).map(n => {
      val ks = new Schema.Parser().parse(new String(root("%s/keySchema".format(toGlobalName(n.name))).data))
      val ns = newIndexNamespace(toGlobalName(n.name), cluster, root, ks)
      ns.open()

      (n.name, (ns, classOf[IndexDefinition].newInstance.parse(ns.getMetadata(indexDefinition).getOrElse(throw new RuntimeException("Invalid index definition in ns: " + namespace + ", " + n.name)))))
    })
  }

  protected def indexCache = indexNamespacesCache
  
  /** Get a list of indexes for this namespace */
  def listIndexes: Map[String, IndexNamespace] = {
    updateCache()
    indexNamespacesCache.map { case (k, v) => (k, v._1) } toMap
  }

  /** getOrCreate a secondary index over the given fields */
  def getOrCreateIndex(fields: Seq[IndexType]): IndexNamespace = {
    //TODO: This encoding is certainly not robust enough to handle all possible field names
    val idxName = fields.map {
      case AttributeIndex(field) => field
      case TokenIndex(fields) => "t" + fields.mkString("t")
    }.mkString("0")

    listIndexes.get(idxName) match {
      case Some(idx) => idx
      case None => createIndex(idxName, fields)
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
  def createIndex(name: String, fields: Seq[IndexType]): IndexNamespace = {
    val prefixFields = fields.map {
      case AttributeIndex(fieldName) => {
        val (l, r) = (keySchema.getField(fieldName), valueSchema.getField(fieldName))
        val origField =
          if (l ne null) l
          else if (r ne null) r
          else throw new IllegalArgumentException("Invalid field name: " + fieldName)
        new Schema.Field(origField.name, origField.schema, "", null)
      }
      case TokenIndex(fieldNames) => {
        fieldNames.foreach(fieldName => {
          val field = Option(valueSchema.getField(fieldName)).getOrElse(throw new IllegalArgumentException("Invalid field name: " + fieldNames))
          if(field.schema.getType != Schema.Type.STRING)
            throw new IllegalArgumentException("Can't build token index over field %s of type %s.".format(fieldNames, field.schema))
        })
        new Schema.Field("Token" + fieldNames.mkString("0"), Schema.create(Schema.Type.STRING), "", null)
      }
    }

    val attrFieldNames = fields.collect {case AttributeIndex(f) => f}
    val suffixFields = keySchema.getFields.filterNot(f => attrFieldNames.contains(f.name)).map(f => new Schema.Field(f.name, f.schema, "", null))
    val indexKeySchema = Schema.createRecord(name + "Key", "", "", false)
    indexKeySchema.setFields(prefixFields ++ suffixFields)


    val indexNs = newIndexNamespace(toGlobalName(name), cluster, root, indexKeySchema)
    // create the actual namespace with no partition strategy here 
    indexNs.open
    val idxDef = IndexDefinition(fields ++ suffixFields.map(f => AttributeIndex(f.name)))
    indexNamespacesCache += ((name, (indexNs, idxDef)))
    indexNs.putMetadata(indexDefinition, idxDef.toBytes)

    // add index catalogue entry- causes other clients to be notified if they
    // are watching
    indexCatalogue.getOrCreate(name)

    // check if data exists already. if so, warn that indexes will NOT be
    // created for existing records
    if (!getRange(None, None, limit=Some(1)).isEmpty)
      logger.warning("WARNING: Indexes will not be created for previous existing records!")

    indexNs  
  }

  protected def makeIndexFor(key: IndexedRecord, value: IndexedRecord, indexKeySchema: Schema, mapping: IndexDefinition): Seq[IndexedRecord] = {
    @inline def getValue(fieldName: String): Any = {
      val idx = key.getSchema.getField(fieldName)
      if(idx != null)
        key.get(idx.pos)
      else
        value.get(value.getSchema.getField(fieldName).pos)
    }

    def buildRecords(pos: Int, records: Seq[GenericData.Record]): Seq[GenericData.Record] = {
      if(pos == mapping.fields.size)
        return records

      val values = mapping.fields(pos) match {
        case AttributeIndex(fieldName) => getValue(fieldName) :: Nil
        case TokenIndex(fieldNames) => fieldNames.flatMap(getValue(_).asInstanceOf[Utf8].toString.split(" ")).filterNot(_.size == 0).map(new Utf8(_)).distinct
      }

      val newRecs = values.flatMap(v => {
        records.map(r => {
          val rec = new GenericData.Record(indexKeySchema)
          (0 to pos).foreach(i => rec.put(i, r.get(i)))
          rec.put(pos, v)
          rec
        })
      })
      buildRecords(pos + 1, newRecs)
    }
    val idxRecords = buildRecords(0, new GenericData.Record(indexKeySchema) :: Nil)
    logger.debug("Generated Index Entries for %s, %s: %s", key, value, idxRecords)
    idxRecords
  }

  /** Delete a pre-existing index. The index named must already exist */
  def deleteIndex(name: String): Unit = {
    listIndexes.get(name).getOrElse(throw new RuntimeException("No such index: " + name)).delete()
    indexNamespacesCache -= name
    indexCatalogue(name).delete()
  }
}
