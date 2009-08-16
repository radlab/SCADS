require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

require 'timeout'
require 'Storage'

module SCADS
  class Client


    def initialize(host,port)
      @@pool ||= {}
      @@pool[host] ||= {}
      @@pool[host][port] ||= Queue.new

      @port = port
      @host = host
    end

    def method_missing(symbol, *args)
      client = nil
      ret = nil
      begin
        client = @@pool[@host][@port].pop(true)
      rescue
        puts "Adding connection to #{@host}:#{@port} to pool" if $DEBUG
        transport = Thrift::FramedTransport.new(Thrift::Socket.new(@host, @port))
        protocol = Thrift::BinaryProtocol.new(transport)
        transport.open
        client = SCADS::Storage::Storage::Client.new(protocol)
      end
      puts "Calling #{symbol} with #{args.inspect}" if $DEBUG

      begin
        Timeout::timeout(5) do
          ret = client.send(symbol, *args)
        end
      rescue Exception =>e
        raise e
      end
      @@pool[@host][@port].push(client)

      return ret
    end
  end
end
