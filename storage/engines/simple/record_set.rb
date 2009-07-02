module SCADS
  class RecordSet
    def includes?(key, value = nil)
      case type
      when RecordSetType::RST_ALL
        true
      when RecordSetType::RST_NONE
        false
      when RecordSetType::RST_RANGE
        (range.start_key.nil? || key >= range.start_key) && (range.end_key.nil? || key <= range.end_key)
      when RecordSetType::RST_KEY_FUNC
        begin
          (eval func.func).call(key)
        rescue Exception => e
          raise InvalidSetDescription.new(:s => self, :info => "Exception running function: #{e.to_s}")
        end
      when RecordSetType::RST_KEY_VALUE_FUNC
        begin
          (eval func.func).call(key, value)
        rescue Exception => e
          raise InvalidSetDescription.new(:s => self, :info => "Exception running function: #{e.to_s}")
        end
      else
        raise NotImplemented.new
      end
    end

    def check_validity
      #check to make sure we don't have extra things set

      if type != RecordSetType::RST_RANGE
        raise InvalidSetDescription.new(:s => self, :info => "range should be empty unless you are doing a range query") if !range.nil?
      elsif type != RecordSetType::RST_KEY_FUNC
        raise InvalidSetDescription.new(:s => self, :info => "func should be empty unless you are doing a user function") if !func.nil?
      end

      case type
      when RecordSetType::RST_ALL
      when RecordSetType::RST_NONE
      when RecordSetType::RST_RANGE
        raise InvalidSetDescription.new(:s => self, :info => "start_key !<= end_key") if (!range.start_key.nil? && !range.end_key.nil?) && range.start_key > range.end_key
        raise InvalidSetDescription.new(:s => self, :info => "offset !<= limit") if (!range.offset.nil? && !range.limit.nil?) && range.offset > range.limit
      when RecordSetType::RST_KEY_FUNC
        raise InvalidSetDescription.new(:s => self, :info => "what language is this!") if func.lang != Language::LANG_RUBY
      when RecordSetType::RST_KEY_VALUE_FUNC
        raise InvalidSetDescription.new(:s => self, :info => "what language is this!") if func.lang != Language::LANG_RUBY
      else
        raise NotImplemented.new
      end

      true
    end
  end
end
