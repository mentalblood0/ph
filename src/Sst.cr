require "yaml"

module Ph
  class Sst
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : String

    @[YAML::Field(ignore: true)]
    getter idx : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter data : File = File.new File::NULL, "r+"

    def after_initialize
      Dir.mkdir_p @path

      @idx = Dir.glob("#{@path}/*.idx").sort.map { |p| File.open p, "r+" }
      @idx.each { |f| f.sync = true }

      @data = (File.open "#{@path}/data", "r+" rescue File.open "#{@path}/data", "w+")
      @data.sync = true
    end

    protected def write_free(til : Pos)
      if @data.pos < til
        ::Log.debug { "write_free from #{@data.pos.to_s 16} til #{til.to_s 16} (#{til - @data.pos} bytes)" }
        Ph.write @data, (Size.new til - @data.pos)
      end
    end

    protected def write_fit(til : Pos, h : Hash(K, V?)) : Kpos
      ::Log.debug { "write_fit from #{@data.pos.to_s 16} til #{til.to_s 16} (#{til - @data.pos} bytes available)" }

      r = Kpos.new

      return r unless @data.pos < til
      fs = til - @data.pos

      h.each do |k, v|
        next unless v
        size = Ph.size k, v
        if size <= fs
          r << {k, Pos.new @data.pos}
          ::Log.debug { "fit #{k.hexstring} #{v.hexstring} at #{@data.pos.to_s 16}" }
          Ph.write @data, k, v

          h.delete k

          fs -= size
          break if fs < 2
        end
      end
      r
    end

    def write(h : Hash(K, V?))
      kpos = Kpos.new
      @data.rewind
      loop do
        begin
          cpos = Pos.new @data.pos
          b = Ph.read @data
          npos = Pos.new @data.pos

          if b.is_a? Free
            ::Log.debug { "found free block of size #{(Ph.size b) + b} at #{(@data.pos - Ph.size b).to_s 16}" }
            @data.pos = cpos
            npos = cpos + b
            kpos += write_fit npos, h
            write_free npos unless @data.pos == cpos
          else
            k = b.as K
            ::Log.debug { "found allocated key block of size #{Ph.size k} at #{(@data.pos - Ph.size b).to_s 16}" }
            v = (Ph.read @data).as V | Nil
            ::Log.debug { "found allocated value block of size #{Ph.size v} at #{(@data.pos - Ph.size v).to_s 16}" }
            npos = Pos.new @data.pos

            if (v.is_a? V) && (h[k] == nil rescue false)
              @data.pos -= Ph.size v
              ::Log.debug { "overwrite block at #{@data.pos.to_s 16} with nil as #{k.hexstring} was deleted" }
              Ph.write @data, nil

              h.delete k

              ::Log.debug { "try fit at newly available space at #{@data.pos.to_s 16}" }
              kpos += write_fit npos, h
              write_free npos
            end
          end

          @data.pos = npos
        rescue IO::EOFError
          break
        end
      end

      unless h.empty?
        @data.seek 0, IO::Seek::End
        datab = IO::Memory.new
        h.each do |k, v|
          next unless v
          kpos << {k, Pos.new @data.pos + datab.pos}
          Ph.write datab, k, v
        end
        @data.write datab.to_slice
      end

      unless kpos.empty?
        idxb = IO::Memory.new
        kpos.sort_by! { |k, _| k }
        kpos.each { |_, pos| Ph.write_pos idxb, pos }

        idxc = File.open "#{@path}/#{(@idx.size.to_u64.to_s 16).rjust 16, '0'}.idx", "w+"
        idxc.sync = true
        idxc.write idxb.to_slice
        @idx << idxc
      end
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

    def get(k : K)
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
            dk = (Ph.read @data).as K

            case c = dk <=> k
            when 0
              @stats.reads += 1
              r = (Ph.read @data).as V?
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
