require "yaml"

module Ph
  class Sst
    include YAML::Serializable
    include YAML::Serializable::Strict

    POS_SIZE = 6_i64

    getter path : String

    @[YAML::Field(ignore: true)]
    getter idx : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    @data : File = File.new File::NULL, "r+"

    def after_initialize
      Dir.mkdir_p @path

      @idx = Dir.glob("#{@path}/*.idx").sort.map { |p| File.open p, "r+" }
      @idx.each { |f| f.sync = true }

      @data = (File.open "#{@path}/data", "r+" rescue File.open "#{@path}/data", "w+")
      @data.sync = true
    end

    def write(h : Hash(Bytes, Bytes?))
      kpos = Array(Tuple(Bytes, UInt64)).new
      @data.rewind
      loop do
        begin
          k = (Ph.read @data).as Bytes
          v = Ph.read @data

          if (v.is_a? Bytes) && (h[k] == nil rescue false)
            if fit = h.find { |k, v| (Ph.size k, v) <= Ph.size v }
              @data.pos -= Ph.size v
              kpos << {fit[0], @data.pos.to_u64!}
              @data.write fit[0], fit[1]

              h.delete fit[0]
            else
              @data.pos -= Ph.size v
              Ph.write @data, nil

              if (Ph.size v) > 0
                Ph.write @data, (Ph.size v) - 1
              else
                @data.skip Ph.size v
              end
            end

            h.delete k
          end
        rescue IO::EOFError
          break
        end
      end

      datab = IO::Memory.new
      h.each { |k, v| Ph.write datab, k, v }
      @data.seek 0, IO::Seek::End
      @data.write datab.to_slice

      idxb = IO::Memory.new
      kpos.sort_by! { |k, _| k }
      kpos.each { |_, pos| Ph.write_pos idxb, pos }

      idxc = File.open Ph.filepath(@path, @idx.size, "idx"), "w+"
      idxc.sync = true
      idxc.write idxb.to_slice
      @idx << idxc
    end

    class Stats
      include YAML::Serializable
      include YAML::Serializable::Strict

      property seeks : UInt64 = 0_u64
      property reads : UInt64 = 0_u64

      def initialize
      end

      def reset
        @seeks = 0_u64
        @reads = 0_u64
      end
    end

    @[YAML::Field(ignore: true)]
    getter stats : Stats = Stats.new

    def get(k : Bytes)
      (@idx.size - 1).downto(0) do |i|
        idxc = @idx[i]

        begin
          l = 0_i64
          r = (idxc.size // POS_SIZE - 1).to_i64!
          while l <= r
            m = l + ((r - l) / 2).floor.to_i64!
            idxc.pos = m * POS_SIZE

            @stats.seeks += 1
            @stats.reads += 1
            @data.seek Ph.read_pos idxc

            @stats.reads += 1
            dk = (Ph.read @data).as Bytes

            case c = dk <=> k
            when 0
              @stats.reads += 1
              r = (Ph.read @data).as Bytes?
              return r
            when .< 0 then l = m + 1
            when .> 0 then r = m - 1
            end
          end
        rescue IO::EOFError
          next
        end
      end
    end
  end
end
