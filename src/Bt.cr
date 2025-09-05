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
      getter k : Bytes { raw[0..(@bt.s - 1)] }
      getter l : P? { decode raw[@bt.s..(@bt.s + @bt.ps - 1)] }
      getter r : P? { decode raw[(@bt.s + @bt.ps)..(@bt.s + 2 * @bt.ps - 1)] }
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
        raw[0..(@bt.s - 1)].copy_from @k.not_nil!
        v = @bt.vf.call @k.not_nil!
      end

      def l=(@l : P?)
        lb = @l ? encode @l : Bytes.new @bt.ps.to_i32!, 255
        raw[@bt.s..(@bt.s + @bt.ps - 1)].copy_from lb
      end

      def r=(@r : P?)
        rb = @r ? encode @r : Bytes.new @bt.ps.to_i32!, 255
        raw[(@bt.s + @bt.ps)..(@bt.s + 2 * @bt.ps - 1)].copy_from rb
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
        return Bytes.new bt.ps.to_i32!, 255 unless p
        r = Bytes.new Math.max 8, bt.ps
        IO::ByteFormat::BigEndian.encode p, r[(Math.max 8, bt.ps) - 8..]
        (bt.ps >= 8) ? r : r[8 - bt.ps..]
      end
    end

    getter vf : Proc(Bytes, Bytes) = Proc(Bytes, Bytes).new { |b| b }

    getter s : UInt8
    getter ps : UInt8
    getter al : Al

    def initialize(@s, @ps, @al, @vf)
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

    def delete(v : Bytes) : Bool
      return false unless root

      # Find the node to delete and its parent
      c = root
      p : Node? = nil
      l = false

      # Search for the node
      while c && c.v != v
        p = c
        if v < c.v
          c = c.left
          l = true
        else
          c = c.right
          l = false
        end
      end

      # Node not found
      return false unless c

      # Handle deletion based on number of children
      case {!c.left.nil?, !c.right.nil?}
      when {false, false}
        # Case 1: Leaf node
        dl p, l
      when {true, false}, {false, true}
        # Case 2: One child
        d1c c, p, l
      else
        # Case 3: Two children
        d2c c
      end

      true
    end

    private def dl(p : Node?, l : Bool)
      if p.nil?
        r = root
        @al.delete r.p if r # Deleting the root
      elsif l
        p.not_nil!.left = nil
      else
        p.not_nil!.right = nil
      end
    end

    private def d1c(n : Node, p : Node?, l : Bool)
      c = (n.left || n.right).not_nil!
      if p.nil?
        r = root.not_nil!
        r.k = c.k
        r.l = c.l
        r.r = c.r
        r.update
      elsif l
        p.not_nil!.left = c
      else
        p.not_nil!.right = c
      end
    end

    private def d2c(n : Node)
      # Find inorder successor (leftmost node in right subtree)
      sp = n
      s = n.right.not_nil!
      sl = false

      # Traverse to find the smallest node in right subtree
      while !s.left.nil?
        sp = s
        s = s.left.not_nil!
        sl = true
      end

      # Copy successor value to current node
      n.k = s.k

      # Remove the successor (which has at most one right child)
      if sl
        sp.not_nil!.left = s.right
      else
        sp.not_nil!.right = s.right
      end
    end
  end
end
