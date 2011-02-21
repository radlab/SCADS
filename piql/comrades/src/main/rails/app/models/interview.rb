class Interview < AvroRecord
  attr_accessor :new_record

  def self.human_name
    "Interview"
  end

  def self.find(email, created_at)
    interview = Interview.find_interview(email, created_at)
    if interview.present?
      interview = interview.first.first
      interview.new_record = false
      interview
    else
      nil
    end
  end
  
  def to_param
    created_at.to_s
  end
  
  def initialize(params={})
    super(params)
    self.new_record = true
  end

  def new_record?
    new_record
  end
end