require "yaml"

require "./common.cr"

module Ph
  class Al
    include YAML::Serializable
    include YAML::Serializable::Strict

    @[YAML::Field(converter: Ph::IOConverter)]
    getter io : IO::Memory | File
    getter s : UInt8

    def after_initialize
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
      r = Bytes.new Math.max 8, @s
      IO::ByteFormat::BigEndian.encode f, r[(Math.max 8, @s) - 8..]
      (@s >= 8) ? r : r[8 - @s..]
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
