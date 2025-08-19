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
      free = Hash(UInt16, Array(UInt64)).new
      begin
        File.open("#{@path}/free") do |freef|
          loop do
            begin
              pos = Ph.read_pos freef
              size = Ph.read_size freef
              free[size] = Array(UInt64).new unless free[size]?
              free[size] << pos
            rescue IO::EOFError
              break
            end
          end
        end
      rescue File::NotFoundError
      end

      undof = File.open "#{@path}/undo", "w"
      undof.sync = true

      ds = @data.size.to_u64
      Ph.write_pos undof, ds

      @data.rewind
      loop do
        begin
          k = Ph.read @data
          size = Ph.read_size @data

          if (size != UInt16::MAX) && (h[k] == nil rescue false)
            pos = @data.pos.to_u64!
            free[size] = Array(UInt64).new unless free[size]?
            free[size] << pos

            @data.pos -= 2

            undob = IO::Memory.new
            Ph.write_pos undob, @data.pos.to_u64!
            Ph.write_size undob, 2_u16
            Ph.write_size undob, size
            undof.write undob.to_slice

            Ph.write_size @data, nil
            h.delete k
          end

          @data.skip size
        rescue IO::EOFError
          break
        end
      end

      datab = IO::Memory.new
      freekvs = free.to_a
      freekvs.sort_by! { |k, _| k }
      kpos = Array(Tuple(Bytes, UInt64)).new
      h.each do |hk, hv|
        ow = false
        freekvs.each do |size, poses|
          next if poses.empty?
          rs = (2 + hk.size + 2 + hv.not_nil!.size).to_u16!
          next unless size >= rs
          @data.pos = poses.pop

          undob = IO::Memory.new
          Ph.write_pos undob, @data.pos.to_u64!
          Ph.write_size undob, rs
          Ph.write undob, hk, hv
          undof.write undob.to_slice

          kpos << {hk, @data.pos.to_u64!}
          Ph.write @data, hk, hv
          ow = true
        end
        next if ow
        kpos << {hk, ds + datab.pos.to_u64}
        Ph.write datab, hk, hv unless ow
      end
      @data.seek 0, IO::Seek::End
      @data.write datab.to_slice

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
            dk = (Ph.read @data).not_nil!

            case c = dk <=> k
            when 0
              @stats.reads += 1
              return Ph.read @data
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
