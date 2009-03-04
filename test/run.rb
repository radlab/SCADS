

$:.push('storage/gen-rb')
$:.push('storage/simple_ruby')

require 'thrift'
require 'thrift/protocol/binaryprotocol'
require 'thrift/server/tserver'

Thread.abort_on_exception = true

class StorageServer
  require 'simple'

  def initialize
    handler = SimpleStorageHandler.new()
    processor = Storage::Processor.new(handler)
    @transport = Thrift::ServerSocket.new(9090)
    transportFactory = Thrift::BufferedTransportFactory.new()
    @server = Thrift::SimpleServer.new(processor, @transport, transportFactory)
    @thread = Thread.new do 
      begin
        @server.serve
      rescue Exception => e
        puts e
        puts e.backtrace
        raise "server died"
      end      
    end

    transport = Thrift::BufferedTransport.new(Thrift::Socket.new('localhost', 9090))
    protocol = Thrift::BinaryProtocol.new(transport)
    transport.open
    @client = Storage::Client.new(protocol)
  end

  def method_missing(symbol, *args)
    @client.send(symbol, *args)
  end
  
  def stop
    @thread.kill
    @transport.close
  end
end

require 'test/ts_basic_storage'
require 'test/ts_sets'
require 'test/ts_syncing'
require 'test/ts_responsibility'

puts "test"