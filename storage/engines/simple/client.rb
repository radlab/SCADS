require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

require 'timeout'
require 'Storage'
["simple", "record_set", "conflict_policy"].each do |file|
   require file
end

module SCADS
  class Client
    
    def initialize(host,port)
      @port = port
      @host = host
    end
    
    def method_missing(symbol, *args)
      puts "Attempting connection to #{@host}:#{@port}" if $DEBUG
      transport = Thrift::FramedTransport.new(Thrift::Socket.new(@host, @port))
      protocol = Thrift::BinaryProtocol.new(transport)
      transport.open
      client = SCADS::Storage::Storage::Client.new(protocol)

      puts "Calling #{symbol} with #{args.inspect}" if $DEBUG
      ret = nil

      begin
        Timeout::timeout(5) do
          ret = client.send(symbol, *args)
        end
      rescue Exception =>e
        transport.close
        raise e
      end

      transport.close
      return ret
    end
  end
end

