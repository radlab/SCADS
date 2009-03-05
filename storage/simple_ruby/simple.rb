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
    raise NotResponsible.new unless @responsibility_policies[ns].includes?(key)
    Record.new(:value => @data[ns][key])
  end
  
  def get_set(ns, rs)
    #check to see if the rs they gave us is valid
    rs.check_validity
    
    rp = @responsibility_policies[ns]
    raise NotResponsible.new if rs.type == RecordSetType::RANGE && (!rp.includes?(rs.range.start_key) || !rp.includes?(rs.range.end_key))
    
    ret = @data[ns].map {|rec| Record.new(:key => rec[0], :value => rec[1])}.select {|rec| rs.includes?(rec.key)}
    
    if rs.type == RecordSetType::RANGE
      ret[((rs.range.start_limit || 0)..(rs.range.end_limit || ret.size))]
    else
      ret
    end
  end
  
	def put(ns, rec)
	  raise NotResponsible.new unless @responsibility_policies[ns].includes?(rec.key)
	  @data[ns][rec.key] = rec.value
	  return true
  end
	
	def set_responsibility_policy(ns, policy)
	  #check to see if the rs they gave us is valid
    policy.check_validity
    raise InvalidSetDescription.new(:s => policy, :info => "start and end limit don't make sence here") if policy.type == RecordSetType::RANGE && (!policy.range.start_limit.nil? || !policy.range.end_limit.nil?)
	  
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