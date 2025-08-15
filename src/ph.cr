require "yaml"

module Ph
  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    module PathToAppendFileConverter
      def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : File
        File.open String.new(ctx, node), "a"
      end
    end

    module PathToReadWriteFileConverter
      def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : File
        p = String.new ctx, node
        begin
          File.open p, "r+"
        rescue File::NotFoundError
          File.open p, "w+"
        end
      end
    end

    getter sync : Bool

    @[YAML::Field(converter: Ph::Env::PathToAppendFileConverter)]
    getter log : File
    @[YAML::Field(converter: Ph::Env::PathToReadWriteFileConverter)]
    getter sst : File
    @[YAML::Field(converter: Ph::Env::PathToReadWriteFileConverter)]
    getter data : File

    @[YAML::Field(ignore: true)]
    @h : Hash(Bytes, Bytes) = Hash(Bytes, Bytes).new

    def read(io : IO)
      r = Bytes.new IO::ByteFormat::BigEndian.decode UInt16, io
      io.read r
      r
    end

    protected def read_log(&)
      File.open @log.path do |f|
        loop do
          begin
            k = read f
            v = read f
            yield({k, v})
          rescue IO::EOFError
            break
          end
        end
      end
    end

    def after_initialize
      @log.sync = @sync
      @sst.sync = @sync
      @data.sync = @sync
      read_log { |k, v| @h[k] = v }
    end

    protected def write(io : IO, o : Bytes)
      IO::ByteFormat::BigEndian.encode o.size.to_u16!, io
      io.write o
    end

    def checkpoint
      keys = @h.keys
      keys.sort!
      posb = Bytes.new 8
      keys.each do |k|
        IO::ByteFormat::BigEndian.encode @data.pos.to_u64!, posb
        @sst.write posb
        write @data, k
        write @data, @h[k]
      end
      @h.clear
    end

    def set(k : Bytes, v : Bytes)
      write @log, k
      write @log, v
      @h[k] = v
    end

    def get(k : Bytes)
      r = @h[k]?
      return r if r

      rs = 8_i64
      @sst.pos = (@sst.size / rs).to_i64 / 2 * rs
      step = @sst.pos / rs / 2
      loop do
        @data.seek IO::ByteFormat::BigEndian.decode UInt64, @sst

        _c = k <=> read @data
        return read @data if _c == 0

        c = _c <= 0 ? _c < 0 ? -1 : 0 : 1
        return nil if step.abs == 1 && c * step < 0

        step = c * step.abs
        if step.abs != 1
          if step.abs < 2
            step = step > 0 ? 1 : -1
          else
            step /= 2
          end
        end

        @sst.pos += step.to_i64 * rs - rs
      end
    end
  end
end
