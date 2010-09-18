package edu.berkeley.cs.scads

import org.apache.avro.generic.{GenericData, IndexedRecord}

package object piql {
  type Namespace = edu.berkeley.cs.scads.storage.GenericNamespace
  type KeyGenerator = Seq[Value]
  
  type Record = GenericData.Record
  type CursorPosition = Seq[Any]
  type Tuple = Array[Record]
  type QueryResult = Seq[Tuple]
}
