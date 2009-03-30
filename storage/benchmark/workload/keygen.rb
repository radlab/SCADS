#!/usr/bin/env ruby
require 'getoptlong'

opts = GetoptLong.new(
  [ '--seed', '-s', GetoptLong::REQUIRED_ARGUMENT ]
)

opts.each do |opt, arg|
  case opt
  when '--seed'
    srand(arg.hash)
  end
end

begin
  loop do
    puts (0...(1+rand(100))).map{(33+rand(93)).chr}.join
  end
rescue Errno::EPIPE => e
  #Consumers will just exit when full
end
