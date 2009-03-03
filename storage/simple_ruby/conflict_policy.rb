class ConflictPolicy
  def which(val1, val2)
    case type
    when ConflictPolicyType::GREATER
      val1 > val2 ? val1 : val2
    else
      raise "UNIMPLEMENTED"
    end
  end
end