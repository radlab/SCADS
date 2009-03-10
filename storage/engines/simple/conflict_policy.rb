module SCADS
  module Storage
    class ConflictPolicy
      def which(val1, val2)
        case type
        when ConflictPolicyType::GREATER
          val1 > val2 ? val1 : val2
        when ConflictPolicyType::FUNC
          begin
            (eval func.func).call(val1,val2)
          rescue Exception => e
            raise InvalidSetDescription.new(:s => self, :info => "Exception running function: #{e.to_s}")
          end
        else
          raise NotImplemented.new
        end
      end
    end
  end
end