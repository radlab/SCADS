package scads.deployment

abstract class Component {
	def boot
	def waitUntilBooted
	def deploy
	def waitUntilDeployed
}
