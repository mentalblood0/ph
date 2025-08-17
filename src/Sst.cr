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
      posb = Bytes.new 8
      kvs.each do |k, v|
        IO::ByteFormat::BigEndian.encode datab.pos.to_u64!, posb
        idxb.write posb[8 - POS_SIZE..]
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

    getter stats : Hash(String, UInt64) = {"seeks_total" => 0_u64,
                                           "seeks_short" => 0_u64,
                                           "seeks_long"  => 0_u64,
                                           "reads"       => 0_u64}

    def reset_stats
      stats.keys.each { |k| stats[k] = 0_u64 }
    end

    def get(k : Bytes)
      posb = Bytes.new 8
      (@idx.size - 1).downto(0) do |i|
        idxc = @idx[i]
        datac = @data[i]

        begin
          idxc.pos = idxc.size / 2 // POS_SIZE * POS_SIZE
          step = Math.max 1_i64, idxc.pos / POS_SIZE
          loop do
            raise IO::EOFError.new unless (idxc.read_fully posb[8 - POS_SIZE..]) == POS_SIZE
            datac.seek IO::ByteFormat::BigEndian.decode UInt64, posb

            _c = k <=> (Ph.read datac).not_nil!
            @stats["reads"] += 1
            return Ph.read datac if _c == 0

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

            posd = step.round * POS_SIZE - POS_SIZE
            if posd != 0
              idxc.pos += posd
              @stats["seeks_total"] += 1
              if step.to_i64.abs == 1
                @stats["seeks_short"] += 1
              else
                @stats["seeks_long"] += 1
              end
            end
          end
        rescue IO::EOFError
          next
        end
      end
    end
  end
end
