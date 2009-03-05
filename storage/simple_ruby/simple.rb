require 'Storage'
require 'conflict_policy'
require 'record_set'

class SimpleStorageHandler
  def initialize
    @data = Hash.new({})
    @responsibility_policies = Hash.new(RecordSet.new(:type => RecordSetType::ALL))
    @conflict_policies = Hash.new(ConflictPolicy.new(:type => ConflictPolicyType::GREATER))
  end
  
  def get(ns, key)
    raise NotResponsible unless @responsibility_policies[ns].includes?(key)
    Record.new(:value => @data[ns][key])
  end
  
  def get_set(ns, rs)
    @data.map {|rec| Record.new(:key => rec[0], :value => rec[1])}.select {|rec| rs.includes?(rec.key)}
  end
  
	def put(ns, rec)
	  raise NotResponsible unless @responsibility_policies[ns].includes?(rec.key)
	  @data[ns][rec.key] = rec.value
	  return true
  end
	
	def set_responsibility_policy(ns, policy)
	  @responsibility_policies[ns] = policy
	  true
  end
  
  def get_responsibility_policy(ns)
    return @responsibility_policies[ns]
  end
  
	def install_conflict_policy(ns, policy)
	  @conflict_policies[ns] = policy
	  return true
	end
	
	def sync_set(ns, rs, h)
	  raise "UNIMPLEMENTED"
  end
  
	def copy_set(ns, rs, h)
	  raise "UNIMPLEMENTED"
  end
  
	def remove_set(ns, rs)
	  raise "UNIMPLEMENTED"
  end
end