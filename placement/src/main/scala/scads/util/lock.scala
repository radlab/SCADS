package edu.berkeley.cs.scads

class WriteLock {
	var locked = false
	var lock_owner:Thread = null

	def lock() = synchronized {
		val current_thread = Thread.currentThread()
		while (locked && lock_owner != current_thread) wait
		locked = true
		lock_owner = current_thread
	}
	def unlock() = synchronized {
		locked = false
		notify
	}
}