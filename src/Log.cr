require "yaml"

module Ph
  class Log
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : Path

    @[YAML::Field(ignore: true)]
    @f : File = File.new File::NULL, "a"

    def after_initialize
      Dir.mkdir_p @path.parent
      @f = File.open path, "a"
      @f.sync = true
    end

    def read(&)
      File.open @f.path do |f|
        loop do
          case (opt = OpT.new f.read_byte.not_nil! rescue break)
          when OpT::DELETE_KEY
            k = (Ph.read f).as K
            yield({k, nil})
          when OpT::DELETE_VALUE
            v = (Ph.read f).as V
            yield({nil, v})
          when OpT::DELETE_KEY_VALUE
            k = (Ph.read f).as K
            v = (Ph.read f).as V
            yield({ {k, v}, nil })
          when OpT::INSERT
            k = (Ph.read f).as K
            v = (Ph.read f).as V
            yield({k, v})
          else
            raise "can not recover operation of type #{opt}"
          end
        end
      end
    end

    def write(ops : Array(Op))
      buf = IO::Memory.new
      ops.each do |op|
        case op
        when {K, Nil}
          k = op[0].as K
          buf.write_byte OpT::DELETE_KEY.value
          Ph.write buf, k
        when {Nil, V}
          v = op[1].as V
          buf.write_byte OpT::DELETE_VALUE.value
          Ph.write buf, v
        when { {K, V}, Nil }
          k, v = op[0].as {K, V}
          buf.write_byte OpT::DELETE_KEY_VALUE.value
          Ph.write buf, k, v
        when {K, V}
          k, v = op[0].as(K), op[1].as(V)
          buf.write_byte OpT::INSERT.value
          Ph.write buf, k, v
        else
          raise "can not commit #{op} of type #{typeof(op)}"
        end
      end
      ::Log.debug { "dump transaction to log" }
      @f.write buf.to_slice
    end

    def truncate
      @f.truncate
    end
  end
end
