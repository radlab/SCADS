package scads.deployment

abstract class Components {
	def boot
	def waitUntilBooted
	def deploy
	def waitUntilDeployed
}
