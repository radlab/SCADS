#!/usr/bin/env ruby
require 'getoptlong'

opts = GetoptLong.new(
  [ '--seed', '-s', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--table', '-t', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--lines', '-n', GetoptLong::REQUIRED_ARGUMENT],
  [ '--data' , '-d', GetoptLong::REQUIRED_ARGUMENT]
)

table = "scads-default"
line_limit = nil # Number of lines of output
data_limit = nil # Approximate total amount of data (in chars) to transfer
seed = rand.hash
opts.each do |opt, arg|
  case opt
  when '--seed'
    seed = arg.hash
  when '--table'
    table = arg
  when '--lines'
    line_limit = arg.to_i
  when '--data'
    data_limit = arg.to_i
  end
end
srand(seed)

$stdin.each_line do |key|
  exit unless line_limit.nil? or line_limit > 0
  exit unless data_limit.nil? or data_limit > 0

  value = (0...(1+rand(500))).map{(33+rand(93)).chr}.join

  if !data_limit.nil?
    data_limit -= value.length
    #If we've overflow, we truncate the value to exactly meet the data limit
    value = value[0...data_limit] if (data_limit < 0)
  end

  puts "put #{table} #{key.chomp} #{value}"

  line_limit -= 1 unless line_limit.nil?
end
