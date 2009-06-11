import edu.berkeley.cs.scads.thrift.StorageEngine

abstract class ObjectPool[PoolType] {
	def borrowObject(): PoolType
	def returnObject(o: PoolType)
}

class SimpleObjectPool[PoolType](c: () => PoolType) extends ObjectPool[PoolType] {
	val pool = new scala.collection.mutable.ArrayStack[PoolType]
	val creator = c

  	def borrowObject(): PoolType = {
  	  if(pool.isEmpty)
  	     c()
      else
         pool.pop
  	}

	def returnObject(o: PoolType) {
	  pool.push(o)
	}
}

object ConnectionPool {
	val connections = new scala.collection.mutable.HashMap[StorageNode, SimpleObjectPool]

	def useConnection[ReturnType](node: StorageNode, f: SCADS.Storage.Client => ReturnType): ReturnType = {
		val conn = connections(node).borrowObject()
		f(conn)
		connections(node).returnObject(conn)
	}
}