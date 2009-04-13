
class DataPlacement {
	val keySpace = new KeySpace
	
	def assign(range: KeyRange, node: StorageNode) = null
	def copy(keyRange: KeyRange, src: StorageNode, dest: StorageNode) = null
	def move(keyRange: KeyRange, src: StorageNode, dest: StorageNode) = null
	def remove(keyRange: KeyRange, node: StorageNode) = null
}