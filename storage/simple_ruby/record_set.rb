class RecordSet
  def includes?(key)
    case type
    when RecordSetType::ALL
      true
    when RecordSetType::NONE
      false
    when RecordSetType::RANGE
      (range.start_key.nil? || key >= range.start_key) && (range.end_key.nil? || key <= range.end_key)
    when RecordSetType::KEY_FUNC
      begin
        (eval func.func).call(key)
      rescue Exception => e
        raise InvalidSetDescription.new(:s => self, :info => "Exception running function: #{e.to_s}")
      end
    else
      raise NotImplemented.new
    end
  end
  
  def check_validity
    #check to make sure we don't have extra things set
    
    if type != RecordSetType::RANGE
      raise InvalidSetDescription.new(:s => self, :info => "range should be empty unless you are doing a range query") if !range.nil?
    elsif type != RecordSetType::KEY_FUNC
      raise InvalidSetDescription.new(:s => self, :info => "func should be empty unless you are doing a user function") if !func.nil?
    end
    
    
    
    case type
    when RecordSetType::ALL
    when RecordSetType::NONE
    when RecordSetType::RANGE
      raise InvalidSetDescription.new(:s => self, :info => "start_key !<= end_key") if (!range.start_key.nil? && !range.end_key.nil?) && range.start_key > range.end_key
      raise InvalidSetDescription.new(:s => self, :info => "start_limit !<= end_limit") if (!range.start_limit.nil? && !range.end_limit.nil?) && range.start_limit > range.end_limit
    when RecordSetType::KEY_FUNC
      raise InvalidSetDescription.new(:s => self, :info => "what language is this!") if func.lang != Language::RUBY
    else
      raise NotImplemented.new
    end
  
    true
  end
end