class GlobalId
	def self.getUniqueID
		# Generates a unique ID
		globalid = Query.globalIdbyId(1)
		globalid.id_counter = globalid.id_counter + 1
		globalid.save
		return globalid.id_counter
	end
end
