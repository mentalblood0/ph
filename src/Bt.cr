require "yaml"

require "./common.cr"
require "./Al.cr"

module Ph
  class Bt
    include YAML::Serializable
    include YAML::Serializable::Strict

    alias P = UInt64
    NIL = UInt64::MAX
    alias Node = {k: Bytes, l: P, r: P}

    @[YAML::Field(converter: Ph::IOConverter)]
    getter io : IO::Memory | File
    getter s : UInt8
    getter vf : Proc(Bytes, Bytes) = Proc(Bytes, Bytes).new { |b| b }

    getter al : Al { Al.new @io, @s * 3 }

    def initialize(@io, @s, @vf)
    end

    protected def encode(f : P) : Bytes
      r = Bytes.new Math.max 8, @s
      IO::ByteFormat::BigEndian.encode f, r[(Math.max 8, @s) - 8..]
      (@s >= 8) ? r : r[8 - @s..]
    end

    protected def decodep(b : Bytes) : P
      r = 0_u64
      b.each { |b| r = (r << 8) + b }
      r
    end

    protected def encode(n : Node) : Bytes
      n[:k] + (encode n[:l]) + (encode n[:r])
    end

    protected def decoden(b : Bytes) : Node
      {k: b[(0 * @s)..(1 * @s - 1)],
       l: (decodep b[(1 * @s)..(2 * @s - 1)]),
       r: (decodep b[(2 * @s)..(3 * @s - 1)])}
    end

    def add(k : Bytes)
      ::Log.debug { "Bt.add #{k.hexstring}" }

      y : Node? = nil
      i = 1_u64
      x = (decoden al.get i rescue nil)

      v = vf.call k
      while x
        y = x
        it = v < (vf.call x[:k]) ? x[:l] : x[:r]
        break if it == 2 ** (8 * @s) - 1
        x = decoden al.get it rescue break
        i = it
      end

      r = al.add encode({k: k, l: NIL, r: NIL})
      if y
        al.replace i, encode v < (vf.call y[:k]) ? {k: y[:k], l: r, r: y[:r]} : {k: y[:k], l: y[:l], r: r}
      end

      r
    end

    def get(v : Bytes)
      ::Log.debug { "Bt.get #{v.hexstring}" }

      i = 1_u64
      while x = (decoden al.get i rescue nil)
        case v <=> vf.call x[:k]
        when .< 0
          i = x[:l]
        when .> 0
          i = x[:r]
        when 0
          return x[:k]
        end
        break if i == 2 ** (8 * @s) - 1
      end
    end
  end
end
