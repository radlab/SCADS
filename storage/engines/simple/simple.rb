require 'Storage'

module SCADS
  module Storage
    module Simple
      class Handler
        def initialize
          @data = Hash.new {|hash, key| hash[key] = {}}
          @responsibility_policies = Hash.new(RecordSet.new(:type => RecordSetType::RST_ALL))
          @conflict_policies = Hash.new(ConflictPolicy.new(:type => ConflictPolicyType::CPT_GREATER))
        end

        def get(ns, key)
          raise NotResponsible.new unless @responsibility_policies[ns].includes?(key)
          Record.new(:key=>key,:value => @data[ns][key])
        end

        def get_set(ns, rs)
          #check to see if the rs they gave us is valid
          rs.check_validity

          rp = @responsibility_policies[ns]
          raise NotResponsible.new if rs.type == RecordSetType::RST_RANGE && (!rp.includes?(rs.range.start_key) || !rp.includes?(rs.range.end_key))

          ret = @data[ns].map {|rec| Record.new(:key => rec[0], :value => rec[1])}.select {|rec| rs.includes?(rec.key, rec.value)}

          if rs.type == RecordSetType::RST_RANGE
            ret[((rs.range.offset || 0)..(rs.range.limit || ret.size))]
          else
            ret
          end.sort{|x,y| x.key <=> y.key}
        end

        def put(ns, rec)
          raise NotResponsible.new unless @responsibility_policies[ns].includes?(rec.key)
          if rec.value.nil?
            @data[ns].delete(rec.key)
          else
            @data[ns][rec.key] = rec.value
          end
          return true
        end

        def count_set(ns, rs)
          #check to see if the rs they gave us is valid
          rs.check_validity

          rp = @responsibility_policies[ns]
          raise NotResponsible.new if rs.type == RecordSetType::RST_RANGE && (!rp.includes?(rs.range.start_key) || !rp.includes?(rs.range.end_key))

          @data[ns].map {|rec| Record.new(:key => rec[0], :value => rec[1])}.select {|rec| rs.includes?(rec.key, rec.value)}.size
        end

        def set_responsibility_policy(ns, policy)
          #check to see if the rs they gave us is valid
          policy.check_validity
          raise InvalidSetDescription.new(:s => policy, :info => "start and end limit don't make sense here") if policy.type == RecordSetType::RST_RANGE && (!policy.range.offset.nil? || !policy.range.limit.nil?)

          @responsibility_policies[ns] = policy
          true
        end

        def get_responsibility_policy(ns)
          return @responsibility_policies[ns]
        end

        def install_conflict_policy(ns, policy)
          @conflict_policies[ns] = policy
          return true
        end

        def sync_set(ns, rs, h, p)
          rs.check_validity

          host = h[/^([^:]*):([^:]*)$/,1]
          port = h[/^([^:]*):([^:]*)$/,2]

          transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          client = Storage::Client.new(protocol)

          remote_data = {}
          client.get_set(ns, rs).each {|r| remote_data[r.key] = r.value}
          local_data = @data[ns]
          result = {}


          (remote_data.keys + local_data.keys).uniq.select{|k| rs.includes?(k)}.each do |key|
            if local_data[key] != remote_data[key]
              if remote_data[key].nil?
                client.put(ns, Record.new(:key => key, :value => local_data[key]))
              elsif local_data[key].nil?
                @data[ns][key] = remote_data[key]
              else
                merged = p.which(local_data[key], remote_data[key])
                client.put(ns, Record.new(:key => key, :value => merged)) unless remote_data[key] == merged
                @data[ns][key] = merged unless local_data[key] == merged

              end
            end
          end

          transport.close
          true
        end

        def copy_set(ns, rs, h)
          rs.check_validity

          host = h[/^([^:]*):([^:]*)$/,1]
          port = h[/^([^:]*):([^:]*)$/,2]

          transport = Thrift::FramedTransport.new(Thrift::Socket.new(host, port))
          protocol = Thrift::BinaryProtocol.new(transport)
          transport.open
          client = Storage::Client.new(protocol)

          @data[ns].select {|key, value| rs.includes?(key)}.each do |key,value|
            client.put(ns, Record.new(:key => key, :value => value))
          end

          transport.close
          true
        end

        def remove_set(ns, rs)
          rs.check_validity

          @data[ns] = @data[ns].select {|key, value| !rs.includes?(key)}.inject({}){|h, r| h[r[0]] = r[1]; h}
          true
        end
      end
    end
  end
end