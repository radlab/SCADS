import Java::EduBerkeleyCsScadsPiql::UserKey
import Java::EduBerkeleyCsScadsPiql::UserValue
import Java::EduBerkeleyCsScadsPiql::ScadrClient
import Java::EduBerkeleyCsScadsPiql::SimpleExecutor
import Java::EduBerkeleyCsScadsStorage::TestScalaEngine

$CLIENT = ScadrClient.new(TestScalaEngine.getTestCluster, SimpleExecutor.new, 10, 10, 10 ,10)

class AvroRecord
  attr_accessor :key
  attr_accessor :value

  def method_missing(method, *args, &block)
    attrname = method.to_s
    scalaMethod = method
    setter = false

    if(method.to_s =~ /(\w*)=/)
      setter = true
      attrname = $1
      scalaMethod = (attrname + "_$eq").intern
    end

    part = nil
    if(!key.getSchema.getField(attrname).nil?)
      part = key
    elsif(!value.getSchema.getField(attrname).nil?)
      part = value
    else
      raise NoMethodError
    end

    if(setter)
     part.send(scalaMethod, args[0])
    else
     part.send(scalaMethod)
    end
  end
end

class User < AvroRecord
  def initialize
    @key = UserKey.new
    @value = UserValue.new
  end

  def save
    $CLIENT.users.put(key, value)
  end
end
