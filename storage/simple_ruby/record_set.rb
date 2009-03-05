class RecordSet
  def includes?(key)
    case type
    when RecordSetType::ALL
      true
    when RecordSetType::NONE
      false
    when RecordSetType::RANGE
      key >= range.start_key && key <= range.end_key
    else
      NotImplemented.new
    end
  end
end