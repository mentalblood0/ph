require "yaml"

require "./common.cr"
require "./Al.cr"

module Ph
  class Bt
    include YAML::Serializable
    include YAML::Serializable::Strict

    class Node
      alias P = UInt64
      NIL = UInt64::MAX

      getter bt : Bt

      getter p : P { @bt.al.add @raw.not_nil! }
      getter raw : Bytes { @bt.al.get @p.not_nil! }
      getter k : Bytes { raw[(0 * @bt.s)..(1 * @bt.s - 1)] }
      getter l : P? { decode raw[(1 * @bt.s)..(2 * @bt.s - 1)] }
      getter r : P? { decode raw[(2 * @bt.s)..(3 * @bt.s - 1)] }
      getter v : Bytes { @bt.vf.call k }

      def initialize(@bt, @p : P)
        @raw = @bt.al.get @p.not_nil!
      end

      def initialize(@bt, @raw)
      end

      def initialize(@bt, @k, @l, @r)
        @raw = @k.not_nil! + (encode @l) + (encode @r)
      end

      def k=(@k : Bytes)
        raw[(0 * @bt.s)..(1 * @bt.s - 1)].copy_from @k.not_nil!
        v = @bt.vf.call @k.not_nil!
      end

      def l=(@l : P?)
        lb = @l ? encode @l : Bytes.new @bt.s.to_i32!, 255
        raw[(1 * @bt.s)..(2 * @bt.s - 1)].copy_from lb
      end

      def r=(@r : P?)
        rb = @r ? encode @r : Bytes.new @bt.s.to_i32!, 255
        raw[(2 * @bt.s)..(3 * @bt.s - 1)].copy_from rb
      end

      def left : Node?
        return Node.new @bt, l if l
      end

      def right : Node?
        return Node.new @bt, r if r
      end

      def left=(n : Node?)
        c = left
        if n
          if c
            c.k = n.k
            c.l = n.l
            c.r = n.r
            c.update
          else
            l = n.add
          end
        elsif c
          @bt.al.delete c.p
          l = nil
        end
      end

      def right=(n : Node?)
        c = right
        if n
          if c
            c.k = n.k
            c.l = n.l
            c.r = n.r
            c.update
          else
            r = n.add
          end
        elsif c
          @bt.al.delete c.p
          r = nil
        end
      end

      def add
        @p = @bt.al.add @raw.not_nil!
      end

      def update
        @bt.al.replace p, @raw.not_nil!
      end

      protected def decode(b : Bytes) : P?
        return nil if b.all? { |b| b == 255 }
        r = 0_u64
        b.each { |b| r = (r << 8) + b }
        r
      end

      protected def encode(p : P?) : Bytes
        return Bytes.new bt.s.to_i32!, 255 unless p
        r = Bytes.new Math.max 8, bt.s
        IO::ByteFormat::BigEndian.encode p, r[(Math.max 8, bt.s) - 8..]
        (bt.s >= 8) ? r : r[8 - bt.s..]
      end
    end

    @[YAML::Field(converter: Ph::IOConverter)]
    getter io : IO::Memory | File
    getter s : UInt8
    getter vf : Proc(Bytes, Bytes) = Proc(Bytes, Bytes).new { |b| b }

    getter al : Al { Al.new @io, @s * 3 }

    def initialize(@io, @s, @vf)
    end

    def root
      Node.new self, 1_u64 rescue nil
    end

    def add(k : Bytes)
      ::Log.debug { "Bt.add #{k.hexstring}" }

      y : Node? = nil
      x = root

      v = vf.call k
      while x
        y = x
        x = case v <=> vf.call x.k
            when .< 0
              x.left
            when .> 0
              x.right
            when 0
              return x
            end
      end

      r = Node.new self, k, nil, nil
      r.add
      if y
        v < (vf.call y.k) ? y.l = r.p : y.r = r.p
        y.update
      end

      r
    end

    def get(v : Bytes)
      ::Log.debug { "Bt.get #{v.hexstring}" }

      x = root
      while x
        xv = vf.call x.k
        case v <=> xv
        when .< 0
          x = x.left
        when .> 0
          x = x.right
        when 0
          return x.k
        end
      end
    end

    def delete(v : Bytes)
      ::Log.debug { "Bt.delete #{v.hexstring}" }

      return unless (r = root)
      if (rn = delete root, v)
        r.l = rn.l
        r.r = rn.r
      end
    end

    private def delete(r : Node?, v : Bytes) : Node?
      return nil unless r

      c = r.not_nil!

      case v <=> c.v
      when .< 0
        c.left = delete c.left, v
      when .> 0
        c.right = delete c.right, v
      when 0
        if !c.left
          return c.right
        elsif !c.right
          return c.left
        else
          s = find_min c.right.not_nil!
          c.k = s.k
          c.right = delete c.right.not_nil!, s.v
        end
      end

      c
    end

    private def find_min(n : Node) : Node
      c = n
      while c.left
        c = c.left.not_nil!
      end
      c
    end
  end
end
