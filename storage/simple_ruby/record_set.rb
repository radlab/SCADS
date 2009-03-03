class RecordSet
  def includes?(key)
    case type
    when RecordSetType::ALL
      true
    when RecordSetType::NONE
      false
    when RecordSetType::RANGE
      key >= start_key && key <= end_key
    else
      raise "UNIMPLEMENTED"
    end
  end
end