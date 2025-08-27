module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = Tuple(K, V)

  struct BitReader
    getter io : IO
    getter b : UInt8 = 0
    getter bs : UInt8 = 0

    def initialize(@io : IO)
    end

    def align
      @bs = 0
    end

    def read_bits(n : UInt64) : UInt64
      r = 0_u64
      bn = n

      while bn > 0
        if @bs == 0
          @b = @io.read_byte.not_nil! rescue raise IO::EOFError.new
          @bs = 8
        end

        btt = Math.min bn, @bs
        br = @bs - btt

        r <<= btt
        r |= (@b >> br) & ((1 << btt) - 1)

        @b &= (1 << br) - 1
        @bs = br
        bn -= btt
      end

      r
    end

    def read_bytes(n : UInt64) : Bytes
      align
      r = Bytes.new n
      @io.read_fully r
      r
    end
  end
end
