class Object
  # Mainly to convert String to Utf8 without having to check if an Object is a String
  # Can help if anything besides a String needs a similar process
  def pickle
    self
  end
end

class String
  def carmelize
    base = self.camelize
    base.slice(0,1).downcase + base.slice(1,base.size)
  end

  # Convert String to Utf8
  def pickle
    Java::OrgApacheAvroUtil.Utf8.new(self)
  end
end

# Wrapper around ListBuffer, to make it more like an array
class ScalaList
  def initialize
    @list_buffer = Java::ScalaCollectionMutable::ListBuffer.new
  end

  def push(item)
    @list_buffer.append{ |foo| foo.apply item.pickle }
  end

  def to_list
    @list_buffer
  end
end

class AvroRecord
  attr_accessor :avro_pair

  # TODO: Enforce primary keys; Does PIQL handle this?
  def self.set_primary_key(key)
  end
  def self.set_primary_keys(*keys)
  end

  # TODO
  def initialize(opts={})
    @is_new_record = true
    begin
      @avro_pair = eval("Java::#{$PIQL_SCHEMA_PACKAGE}::#{self.class.to_s}").new
    # Catches if the class does not exist
    rescue NameError => e
      raise e, "#{e.message} -- does #{self.class} exist in the schema?"
    end

    opts.each_pair do |col, val|
      self.send("#{col}=".to_sym, val)
    end
  end

  def self.create!(opts={})
    obj = new(opts)
    obj.save
    return obj
  end

  def self.fetch(opts={})
    record = self.new(opts)
    record.set_stale
    record
  end

  # TODO
  def valid?
    true
  end

  def new_record?
    @is_new_record
  end

  # Create record
  def save
    begin
      $CLIENT.send(self.class.to_s.downcase.pluralize).put(avro_pair.key, avro_pair.value)
      @is_new_record = false
      true
    rescue NativeException => e
      puts "exception was thrown"
      puts e
      false
    end
  end

  # Delete record
  def destroy
    begin
      $CLIENT.send(self.class.to_s.downcase.pluralize).put(avro_pair.key, nil)
      true
    rescue NativeException => e
      puts "exception was thrown"
      puts e
      false
    end
  end

  def self.respond_to?(method_id, include_private=false)
    if $CLIENT.respond_to?(method_id)
      true
    else
      super
    end
  end

  # TODO: Either pre-define or cache these methods?
  def self.method_missing(method_id, *arguments, &block)
    # Find
    if $CLIENT.respond_to?(method_id)
      sl = ScalaList.new
      arguments.each { |arg| sl.push(arg) }

      begin
        raw_results = $CLIENT.send(method_id).apply { sl.to_list }
      rescue Exception => e
        puts "exception was thrown"
        puts e
        raw_results = []
      end
      results = []

      while results.size < raw_results.size do
        rows = raw_results.apply(results.size)
        # instance = self.fetch

        tuple = []

        reg = /^(.*)Key$/
        indexQuery = reg.match(rows.apply(0).schema.name) ? true : false
        joinQuery = rows.apply(0).schema.name == "WordListWord" ? true : false
        if indexQuery or joinQuery
          sl = ScalaList.new
          sl.push(rows.apply(1))
          rows = sl.to_list #extract field from the index record
        end

        (0...rows.size).each do |row_index|
          row = rows.apply(row_index)
          schema = row.schema
          fields = schema.fields

          if defined?(schema.name.constantize)
              instance = schema.name.constantize.new
              (0...fields.size).each do |field_index|
                field = fields.get(field_index).name
                value = row.send(field)
                value = value.to_s if value.is_a?(Java::OrgApacheAvroUtil::Utf8)
                instance.send(field.underscore+"=", value)
              end
              tuple.push(instance)
          end
        end


        results.push tuple

      end
      results
    else
      super
    end
  end

  def respond_to?(method_id, include_private=false)
    method_id.to_s =~ /^(\w+)(=)?/
    column_name = $1.carmelize
    if avro_pair.schema.get_field(column_name)
      true
    else
      super
    end
  end

  # TODO: Should be able to "cache" these calls by defining methods on-the-fly
  def method_missing(method_id, *arguments, &block)
    # Column accessor methods
    method_id.to_s =~ /^(\w+)(=)?/
    column_name = $1.carmelize
    method_name = $2 ? (column_name + "_$eq") : column_name

    # Key/Value
    if avro_pair.schema.get_field(column_name)
      avro_pair.send(method_name, *arguments, &block)
    # Default
    else
      super
    end
  end

  def set_stale
    @is_new_record = false
  end

  def set_fresh
    @is_new_record = true
  end
end
