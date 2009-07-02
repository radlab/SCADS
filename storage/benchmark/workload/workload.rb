require 'getoptlong'
require 'util/random'

opts = GetoptLong.new(
  [ '--seed', '-s', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--table', '-t', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--operation', '-o', GetoptLong::REQUIRED_ARGUMENT],
  [ '--number', '-n', GetoptLong::REQUIRED_ARGUMENT]
)

seed = rand
table = "default"
op = "put"
number =
opts.each do |opt, arg|
  case opt
  when '--seed'
    seed = arg
  when '--table'
    table = arg
  when '--operation'
    op = arg
  end
end

keyrand = Random.new(seed)

#Create a string of random ascii characters
def random_string(length, src)
  (0...length).map{(33+keyrand.next(93)).chr}.join
end

# Since we want to generate the same sequence of keys for the same seed, irrespective of the operation being performed
# we reset the seed for each line in a deterministic fashion - ruby doesn't have independent randomness sources, it uses 'rand'
# (Note! this only works here since the key is the first thing generated using 'rand')
loop do
  puts "#{op} #{table} #{random_string(1+rand(20))}"
#  puts "#{op} #{table} #{random_string(1+rand(20))} #{random_string(1+rand(20))}"
  srand(nextseed)
end
