require "math"
require "yaml"

require "./common.cr"

module Ph
  class Log
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : Path

    @[YAML::Field(ignore: true)]
    getter f = File.new File::NULL, "a"

    @[YAML::Field(ignore: true)]
    @br = BitReader.new File.new File::NULL, "r"

    def after_initialize
      Dir.mkdir_p @path.parent
      @f = File.open path, "a"
      @f.sync = true

      @br = BitReader.new File.open path, "r"
    end

    protected def write(io : IO, opt : OpT, k : Bytes)
      raise "too big" if k.size > 2 ** 15 - 1

      s = k.size.to_u32
      ss = (32 - s.leading_zeros_count).to_u32

      rr = (opt.value.to_u32 << (32 - 2)) |
           (ss << (32 - 6)) |
           (s << (32 - 6 - ss))

      r = Bytes.new 4
      IO::ByteFormat::BigEndian.encode rr, r

      rrs = (2 + 4 + ss - 1) // 8
      ::Log.debug { "Log.write #{opt} #{k.hexstring} (size is 0b#{k.size.to_s 2}) header: " + r[..rrs].map { |b| (b.to_s 2).rjust 8, '0' }.join ' ' }
      io.write r[..rrs]

      io.write k
    end

    protected def write(io : IO, opt : OpT, k : Bytes, v : Bytes)
      raise "too big" if k.size > 2 ** 15 - 1
      raise "too big" if v.size > 2 ** 15 - 1

      ::Log.debug { "Log.write #{opt} #{k.hexstring} #{v.hexstring}" }

      ks = k.size.to_u64
      kss = (64 - ks.leading_zeros_count).to_u64

      vs = v.size.to_u64
      vss = (64 - vs.leading_zeros_count).to_u64

      rr = (opt.value.to_u64 << (64 - 2)) |
           (kss << (64 - 6)) |
           (ks << (64 - 6 - kss)) |
           (vss << (64 - 6 - kss - 4)) |
           (vs << (64 - 6 - kss - 4 - vss))

      r = Bytes.new 8
      IO::ByteFormat::BigEndian.encode rr, r

      rrs = (2 + 4 + kss + 4 + vss - 1) // 8
      io.write r[..rrs]

      io.write k
      io.write v
    end

    def write(ops : Array(Op))
      buf = IO::Memory.new
      ops.each do |op|
        case op
        when {K, Nil}
          k = op[0].as K
          write buf, OpT::DELETE_KEY, k
        when {Nil, V}
          v = op[1].as V
          write buf, OpT::DELETE_VALUE, v
        when { {K, V}, Nil }
          k, v = op[0].as {K, V}
          write buf, OpT::DELETE_KEY_VALUE, k, v
        when {K, V}
          k, v = op[0].as(K), op[1].as(V)
          write buf, OpT::INSERT, k, v
        else
          raise "can not commit #{op} of type #{typeof(op)}"
        end
      end
      ::Log.debug { "dump transaction to log: #{buf.to_slice.map { |b| (b.to_s 2).rjust 8, '0' }.join ' '}" }
      @f.write buf.to_slice
    end

    def read(&)
      File.open @f.path do |f|
        loop do
          optv = @br.read_bits 2 rescue break
          case opt = OpT.new optv.to_u8
          when OpT::DELETE_KEY
            ks = @br.read_bits @br.read_bits 4
            k = @br.read_bytes ks
            yield({k, nil})
          when OpT::DELETE_VALUE
            vs = @br.read_bits @br.read_bits 4
            v = @br.read_bytes vs
            yield({nil, v})
          when OpT::DELETE_KEY_VALUE
            ks = @br.read_bits @br.read_bits 4
            vs = @br.read_bits @br.read_bits 4
            k = @br.read_bytes ks
            v = @br.read_bytes vs
            yield({ {k, v}, nil })
          when OpT::INSERT
            ks = @br.read_bits @br.read_bits 4
            vs = @br.read_bits @br.read_bits 4
            k = @br.read_bytes ks
            v = @br.read_bytes vs
            yield({k, v})
          else
            raise "can not read operation of type #{opt}"
          end
        end
      end
    end

    def read
      r = Array(Op).new
      read { |op| r << op }
      r
    end

    def truncate
      @f.truncate
    end
  end
end
