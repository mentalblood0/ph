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
    getter data : Array(File) = [] of File

    def after_initialize
      Dir.mkdir_p @path

      @idx = Dir.glob("#{@path}/*.idx").sort.map { |p| File.open p, "r+" }
      @idx.each { |f| f.sync = true }

      @data = Dir.glob("#{@path}/*.dat").sort.map { |p| File.open p, "r+" }
      @data.each { |f| f.sync = true }
    end

    def write(h : Hash(Bytes, Bytes?))
      kvs = h.to_a
      kvs.sort_by! { |k, _| k }
      idxb = IO::Memory.new
      datab = IO::Memory.new

      # --------------------------------------------------------

      free = Array(NamedTuple(pos: UInt64, size: UInt16)).new
      begin
        File.open("#{@path}/free") do |freef|
          pos = Ph.read_pos freef
          size = IO::ByteFormat::BigEndian.decode UInt16, freef
          free << {pos: pos, size: size}
        end
      rescue File::NotFoundError
      end

      undof = File.open "#{@path}/undo", "w"

      dataf = @data.last
      Ph.writes dataf.size.to_u64, undof
      dataf.rewind
      loop do
        begin
          k = Ph.read dataf
          vs = IO::ByteFormat::BigEndian.decode UInt16, dataf

          if (vs != UInt16.MAX) && (h[k] == nil rescue false)
            pos = dataf.pos.to_u64!
            free << {pos: pos, size: vs}
            dataf.pos -= 2
            Ph.write_size undof, 2_u16
            Ph.write_size dataf, nil
          end

          dataf.skip vs
        rescue IO::EOFError
          break
        end
      end

      # /-------------------------------------------------------

      kvs.each do |k, v|
        Ph.write_pos idxb, datab.pos.to_u64!, POS_SIZE
        Ph.write datab, k
        Ph.write datab, v
      end

      datac = File.open Ph.filepath(@path, @data.size, "dat"), "w+"
      datac.sync = true
      datac.write datab.to_slice
      @data << datac

      idxc = File.open Ph.filepath(@path, @idx.size, "idx"), "w+"
      idxc.sync = true
      idxc.write idxb.to_slice
      @idx << idxc
    end

    class Stats
      include YAML::Serializable
      include YAML::Serializable::Strict

      getter seeks_short : UInt64 = 0_u64
      getter seeks_long : UInt64 = 0_u64
      getter seeks_total : UInt64 { seeks_short + seeks_long }
      property reads : UInt64 = 0_u64

      def initialize
      end

      def add_seek(posd : Int64)
        if posd.abs == POS_SIZE
          @seeks_short += 1
        else
          @seeks_long += 1
        end
      end

      def reset
        @seeks_short = 0_u64
        @seeks_long = 0_u64
        @reads = 0_u64
      end
    end

    @[YAML::Field(ignore: true)]
    getter stats : Stats = Stats.new

    def get(k : Bytes)
      (@idx.size - 1).downto(0) do |i|
        idxc = @idx[i]
        dataf = @data.last

        begin
          idxc.pos = idxc.size / 2 // POS_SIZE * POS_SIZE
          step = Math.max 1_i64, idxc.pos / POS_SIZE
          loop do
            dataf.seek Ph.read_pos idxc

            _c = k <=> (Ph.read dataf).not_nil!
            @stats.reads += 1
            return Ph.read dataf if _c == 0

            c = _c <= 0 ? _c < 0 ? -1 : 0 : 1
            raise IO::EOFError.new if step.abs == 1 && c * step < 0

            step = c * step.abs
            if step.abs != 1
              if step.abs < 2
                step = step > 0 ? 1 : -1
              else
                step /= 2
              end
            end

            posd = (step.round * POS_SIZE - POS_SIZE).to_i64!
            if posd != 0
              idxc.pos += posd
              @stats.add_seek posd
            end
          end
        rescue IO::EOFError
          next
        end
      end
    end
  end
end
