#As of 2009-03-28, this client is purely for speed comparisons - minimal error handling is performed
require 'getoptlong'
require 'gen-rb/Storage'

opts = GetoptLong.new(
  [ '--host', '-h', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--port', '-p', GetoptLong::REQUIRED_ARGUMENT ]
)

host = "localhost"
port = 9090
opts.each do |opt, arg|
  case opt
    when '--host'
      host = arg
    when '--port'
      port = arg.to_i
  end
end

socket = Thrift::Socket.new(host, port);
transport = Thrift::BufferedTransport.new(socket);
protocol = Thrift::BinaryProtocol.new(transport)
transport.open
client = SCADS::Storage::Storage::Client.new(protocol)

$stdin.each_line do |line|
  line = line.split
  case line[0]
  when "get":
    before = Time.now
    result = client.get(line[1], line[2]).value
    after = Time.now
  when "put":
    r = SCADS::Storage::Record.new
    r.key = line[2]
    r.value = line[3]
    before = Time.now
    result = client.put(line[1], r)
    after = Time.now
  else
    raise "Invalid command: #{line[0]}"
  end
  puts (after - before).to_s + "\t" + line.join("\t") + "\t=>\t" + result.to_s
end

transport.close