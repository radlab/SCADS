
trait DataPlacement {
	def assign(node: StorageNode, range: KeyRange)
	def copy(keyRange: KeyRange, src: StorageNode, dest: StorageNode)
	def move(keyRange: KeyRange, src: StorageNode, dest: StorageNode)
	def remove(keyRange: KeyRange, node: StorageNode)
}



class SimpleDataPlacement(ns: String) extends SimpleKeySpace with ThriftConversions with DataPlacement {
	val nameSpace = ns
	val conflictPolicy = new SCADS.ConflictPolicy()
	conflictPolicy.setType(SCADS.ConflictPolicyType.CPT_GREATER)

	override def assign(node: StorageNode, range: KeyRange) {
		super.assign(node, range)
		node.set_responsibility_policy(nameSpace, range)
	}

	def copy(keyRange: KeyRange, src: StorageNode, dest: StorageNode) {
		val newDestRange = lookup(dest) + keyRange

		//Verify the src has our keyRange
		assert((lookup(src) & keyRange) == keyRange)

		//Tell the servers to copy the data
		src.copy_set(nameSpace, keyRange, dest.syncHost)

		//Change the assigment
		assign(dest, newDestRange)

		//Sync keys that might have changed
		src.sync_set(nameSpace, keyRange, dest.syncHost, conflictPolicy)
	}

	def move(keyRange: KeyRange, src: StorageNode, dest: StorageNode) {
		val newSrcRange = lookup(src) - keyRange
		val newDestRange = lookup(dest) + keyRange

		src.copy_set(nameSpace, keyRange, dest.syncHost)
		assign(dest, newDestRange)
		assign(src, newSrcRange)

		src.sync_set(nameSpace, keyRange, dest.syncHost, conflictPolicy)
		src.remove_set(nameSpace, keyRange)
	}

	def remove(keyRange: KeyRange, node: StorageNode) {
		val newRange = lookup(node) - keyRange

		assign(node, newRange)
		lookup(keyRange).foreach((n) => {
			node.sync_set(nameSpace, n._2, n._1.syncHost, conflictPolicy)
		})
		node.remove_set(nameSpace, keyRange)
	}
}