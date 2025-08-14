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

    protected def read_log(&)
      File.open @log.path do |f|
        loop do
          begin
            ks = IO::ByteFormat::BigEndian.decode UInt16, f
            k = Bytes.new ks
            f.read k

            vs = IO::ByteFormat::BigEndian.decode UInt16, f
            v = Bytes.new vs
            f.read v

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
      keys.each do |k|
        posb = Bytes.new 8
        IO::ByteFormat::BigEndian.encode @data.pos.to_u16!, posb
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
      rh = @h[k]?
      return rh if rh
    end
  end
end
