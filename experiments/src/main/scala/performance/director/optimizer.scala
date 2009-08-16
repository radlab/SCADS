package scads.director

abstract class CostAssessor {
	def cost(config:SCADSconfig):Double
}

abstract class Optimizer {
	def coster: CostAssessor
	def selector:ActionSelector
	
	/**
	* Given the current systen configuration and performance, 
	* determine an optimal set of actions to perform on this state
	*/
	def optimize(state:SCADSState): List[Action]
}
