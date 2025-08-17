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
      @data = [File.open Ph.filepath(@path, 0, "dat"), "w+"] if @data.empty?
      @data.each { |f| f.sync = true }
    end

    def write(h : Hash(Bytes, Bytes?))
      free = Hash(UInt16, Array(UInt64)).new
      begin
        File.open("#{@path}/free") do |freef|
          pos = Ph.read_pos freef
          size = Ph.read_size freef
          free[size] = Array(UInt64).new unless free[size]?
          free[size] << pos
        end
      rescue File::NotFoundError
      end

      undof = File.open "#{@path}/undo", "w"
      undof.sync = true

      dataf = @data.last

      ds = dataf.size.to_u64
      Ph.write_pos undof, ds

      dataf.rewind
      loop do
        begin
          k = Ph.read dataf
          size = Ph.read_size dataf

          if (size != UInt16::MAX) && (h[k] == nil rescue false)
            pos = dataf.pos.to_u64!
            free[size] = Array(UInt64).new unless free[size]?
            free[size] << pos

            dataf.pos -= 2

            undob = IO::Memory.new
            Ph.write_pos undob, dataf.pos.to_u64!
            Ph.write_size undob, 2_u16
            Ph.write_size undob, size
            undof.write undob.to_slice

            Ph.write_size dataf, nil
            h.delete k
          end

          dataf.skip size
        rescue IO::EOFError
          break
        end
      end

      idxb = IO::Memory.new
      datab = IO::Memory.new
      freekvs = free.to_a
      freekvs.sort_by! { |k, _| k }
      h.each do |hk, hv|
        ow = false
        freekvs.each do |size, poses|
          next if poses.empty?
          rs = (2 + hk.size + 2 + hv.not_nil!.size).to_u16!
          next unless size >= rs
          dataf.pos = poses.pop

          undob = IO::Memory.new
          Ph.write_pos undob, dataf.pos.to_u64!
          Ph.write_size undob, rs
          Ph.write undob, hk, hv
          undof.write undob.to_slice

          Ph.write_pos idxb, dataf.pos.to_u64!
          Ph.write dataf, hk, hv
          ow = true
        end
        Ph.write_pos idxb, ds + datab.pos.to_u64!
        Ph.write datab, hk, hv unless ow
      end
      dataf.seek 0, IO::Seek::End
      dataf.write datab.to_slice

      undof.close

      freeb = IO::Memory.new
      freekvs.each do |size, poses|
        poses.each do |pos|
          Ph.write_pos freeb, pos
          Ph.write_size freeb, size
        end
      end
      File.write "#{@path}/free", freeb unless freeb.empty?

      undof.delete

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
      puts "get #{k.hexstring}"
      (@idx.size - 1).downto(0) do |i|
        idxc = @idx[i]
        dataf = @data.last

        begin
          idxc.pos = idxc.size / 2 // POS_SIZE * POS_SIZE
          step = Math.max 1_i64, idxc.pos / POS_SIZE
          loop do
            dataf.seek Ph.read_pos idxc

            dk = (Ph.read dataf).not_nil!
            puts "dk = #{dk.hexstring}"
            _c = k <=> dk
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
