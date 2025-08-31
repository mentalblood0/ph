require "yaml"

require "./common.cr"

module Ph
  class Log
    include YAML::Serializable
    include YAML::Serializable::Strict

    @[YAML::Field(converter: Ph::IOConverter)]
    getter io : IO::Memory | File

    protected def write_size(bw : BitWriter, b : Bytes)
      s = b.size.to_u64
      ss = 64_u64 - s.leading_zeros_count
      bw.write_bits ss, 4
      bw.write_bits s, ss
    end

    def write(ops : Array(Op))
      buf = IO::Memory.new
      bw = BitWriter.new buf
      ops.each do |op|
        case op
        when {K, Nil}
          bw.write_bits OpT::DELETE_KEY.value.to_u64, 2
          k = op[0].as K

          write_size bw, k

          bw.write_bytes k
        when {Nil, V}
          bw.write_bits OpT::DELETE_VALUE.value.to_u64, 2
          v = op[1].as V

          write_size bw, v

          bw.write_bytes v
        when { {K, V}, Nil }
          bw.write_bits OpT::DELETE_KEY_VALUE.value.to_u64, 2
          k, v = op[0].as {K, V}

          write_size bw, k
          write_size bw, v

          bw.write_bytes k
          bw.write_bytes v
        when {K, V}
          bw.write_bits OpT::INSERT.value.to_u64, 2
          k, v = op[0].as(K), op[1].as(V)

          write_size bw, k
          write_size bw, v

          bw.write_bytes k
          bw.write_bytes v
        else
          raise "can not commit #{op} of type #{typeof(op)}"
        end
      end
      ::Log.debug { "dump transaction to log: #{buf.to_slice.map { |b| (b.to_s 2).rjust 8, '0' }.join ' '}" }
      @io.write buf.to_slice
    end

    def read(&)
      @io.pos = 0
      br = BitReader.new @io
      loop do
        optv = br.read_bits 2 rescue break
        case opt = OpT.new optv.to_u8
        when OpT::DELETE_KEY
          ks = br.read_bits br.read_bits 4
          k = br.read_bytes ks
          yield({k, nil})
        when OpT::DELETE_VALUE
          vs = br.read_bits br.read_bits 4
          v = br.read_bytes vs
          yield({nil, v})
        when OpT::DELETE_KEY_VALUE
          ks = br.read_bits br.read_bits 4
          vs = br.read_bits br.read_bits 4
          k = br.read_bytes ks
          v = br.read_bytes vs
          yield({ {k, v}, nil })
        when OpT::INSERT
          ks = br.read_bits br.read_bits 4
          vs = br.read_bits br.read_bits 4
          k = br.read_bytes ks
          v = br.read_bytes vs
          yield({k, v})
        else
          raise "can not read operation of type #{opt}"
        end
      end
    end

    def read
      r = Array(Op).new
      read { |op| r << op }
      r
    end

    def truncate
      @io.truncate
    end
  end
end
