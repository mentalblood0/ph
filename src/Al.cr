require "./common.cr"

module Ph
  class Al
    getter io : IO
    getter s : UInt8

    def initialize(@io, @s)
      @io.pos = 0
      return unless read.all? { |b| b == 255 } rescue nil
      io.write Bytes.new @s.to_i32!, 255
    end

    protected def read
      r = Bytes.new @s
      @io.read_fully r
      r
    end

    protected def as_free(b : Bytes)
      r = 0_u64
      b.each { |b| r = (r << 8) + b }
      r
    end

    protected def as_block(f : UInt64)
      r = Bytes.new 8
      IO::ByteFormat::BigEndian.encode f, r
      r[8 - s..]
    end

    def get(i : UInt64)
      @io.pos = i * @s
      read
    end

    protected def set(i : UInt64, b : Bytes)
      @io.pos = i * @s
      @io.write b
    end

    def add(b : Bytes)
      ::Log.debug { "Al.add #{b.hexstring}" }

      @io.pos = 0
      h = read
      if h.all? { |b| b == 255 }
        @io.seek 0, IO::Seek::End
        r = @io.pos.to_u64! // @s
        @io.write b

        r
      else
        r = as_free h
        n1 = get r

        set r, b
        set 0, n1

        r
      end
    end

    def delete(i : UInt64)
      ::Log.debug { "Al.delete #{i}" }

      set i, get 0
      set 0, as_block i
    end
  end
end
