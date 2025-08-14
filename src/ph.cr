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

    def decode(f : File)
      rs = IO::ByteFormat::BigEndian.decode UInt16, f
      r = Bytes.new rs
      f.read r
      r
    end

    protected def read_log(&)
      File.open @log.path do |f|
        loop do
          begin
            k = decode f
            v = decode f
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

    def record(k : Bytes, v : Bytes)
      r = Bytes.new (2 + k.size + 2 + v.size).to_u16

      IO::ByteFormat::BigEndian.encode k.size.to_u16!, r
      k.copy_to r[2..]

      IO::ByteFormat::BigEndian.encode v.size.to_u16!, r[2 + k.size..]
      v.copy_to r[2 + k.size + 2..]
      r
    end

    def checkpoint
      keys = @h.keys
      keys.sort!
      posb = Bytes.new 8
      keys.each do |k|
        IO::ByteFormat::BigEndian.encode @data.pos.to_u64!, posb
        @sst.write posb
        @data.write record k, @h[k]
      end
      @h.clear
    end

    def set(k : Bytes, v : Bytes)
      @log.write record k, v
      @h[k] = v
    end

    def get(k : Bytes)
      r = @h[k]?
      return r if r

      rs = 8_u64
      @sst.pos = ((@sst.size / rs) / 2 * rs).to_i64!
      step = @sst.pos / rs / 2
      loop do
        i = IO::ByteFormat::BigEndian.decode UInt64, @sst
        @data.seek i
        dk = decode @data
        if k < dk
          return nil if step.abs == 1 && step > 0
          step = -1 * step.abs
        elsif k > dk
          return nil if step.abs == 1 && step < 0
          step = step.abs
        else
          return decode @data
        end
        @sst.pos += step.to_i64! * rs - rs
        if step.abs < 2
          step /= step.abs
        else
          step /= 2
        end
      end
    end
  end
end
