require "yaml"

require "./common.cr"
require "./Al.cr"

module Ph
  class Bt
    include YAML::Serializable
    include YAML::Serializable::Strict

    alias P = UInt64
    NIL = UInt64::MAX
    alias Node = {c: Bytes, l: P, r: P}

    @[YAML::Field(converter: Ph::IOConverter)]
    getter io : IO::Memory | File
    getter s : UInt8
    getter kf : Proc(Bytes, Bytes) = Proc(Bytes, Bytes).new { |b| b }

    getter al : Al { Al.new @io, @s * 3 }

    def initialize(@io, @s, @kf)
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
      n[:c] + (encode n[:l]) + (encode n[:r])
    end

    protected def decoden(b : Bytes) : Node
      {c: b[(0 * @s)..(1 * @s - 1)],
       l: (decodep b[(1 * @s)..(2 * @s - 1)]),
       r: (decodep b[(2 * @s)..(3 * @s - 1)])}
    end

    # 1    BST-Insert(T, z)
    # 2      y := NIL
    # 3      x := T.root
    # 4      while x ≠ NIL do
    # 5        y := x
    # 6        if z.key < x.key then
    # 7          x := x.left
    # 8        else
    # 9          x := x.right
    # 10       end if
    # 11     repeat
    # 12     z.parent := y
    # 13     if y = NIL then
    # 14       T.root := z
    # 15     else if z.key < y.key then
    # 16       y.left := z
    # 17     else
    # 18       y.right := z
    # 19     end if

    def add(b : Bytes)
      ::Log.debug { "Bt.add #{b.hexstring}" }

      r = al.add encode({c: b, l: NIL, r: NIL})

      y : Node? = nil
      i = 1_u64
      x = (decoden al.get i rescue nil)

      while x
        y = x
        x = (decoden al.get (kf.call b) < (kf.call x[:c]) ? x[:l] : x[:r] rescue nil)
      end

      if y
        al.replace i, encode (kf.call b) < (kf.call y[:c]) ? {c: y[:c], l: r, r: y[:r]} : {c: y[:c], l: y[:l], r: r}
      end

      r
    end
  end
end
