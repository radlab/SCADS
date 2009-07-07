package edu.berkeley.cs.scads.placement

trait KeySpaceProvider {
	def getKeySpace(ns: String): KeySpace
	def refreshKeySpace()
}

