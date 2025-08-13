require "yaml"

module Ph
  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    module PathToLogConverter
      def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : File
        File.open String.new(ctx, node), "a+"
      end
    end

    @[YAML::Field(converter: Ph::Env::PathToLogConverter)]
    getter log : File

    getter sync : Bool

    @[YAML::Field(ignore: true)]
    @h : Hash(Bytes, Bytes) = Hash(Bytes, Bytes).new

    def after_initialize
      @log.sync = @sync
      File.open @log.path do |f|
        loop do
          begin
            ks = IO::ByteFormat::BigEndian.decode UInt16, f
            k = Bytes.new ks
            f.read k

            vs = IO::ByteFormat::BigEndian.decode UInt16, f
            v = Bytes.new vs
            f.read v

            @h[k] = v
          rescue IO::EOFError
            break
          end
        end
      end
    end

    def set(k : Bytes, v : Bytes)
      r = Bytes.new (2 + k.size + 2 + v.size).to_u16

      IO::ByteFormat::BigEndian.encode k.size.to_u16!, r
      k.copy_to r[2..]

      IO::ByteFormat::BigEndian.encode v.size.to_u16!, r[2 + k.size..]
      v.copy_to r[2 + k.size + 2..]

      @log.write r
      @h[k] = v
    end

    def get(k : Bytes)
      @h[k]?
    end
  end
end
