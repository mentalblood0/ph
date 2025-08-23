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
      return nil unless @data.pos < til

      s = Size.new til - @data.pos
      ::Log.debug { "write_free from #{@data.pos.to_s 16} til #{til.to_s 16} (#{s} bytes)" }
      Ph.write @data, s
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

    # sk ~ immutable, indexes of key-value positions pairs sorted by key
    # sv ~ immutable, indexes of key-value positions pairs sorted by value
    # idx ~ mutable, key-value positions pairs
    # data ~ mutable, keys and values
    #
    # sk -> idx -> data
    # sv -> idx -> data
    # sk, sv = [{i: 5 byte}, ...]
    # idx = [{kp: 6 byte, vp: 6 byte}, ...]
    # data = [block | free, ..., block]
    #
    # kp, vp = idx.read 12 * sk.read 5
    # k = data.read_block kp
    # v = data.read_block vp
    #
    #
    # d : Set(Tuple(Bytes, Bytes)) # deleted
    # u : Hash(Tuple(Bytes, Bytes), Tuple(Bytes, Bytes)) # updated
    # ik : Hash(Bytes, Array(Bytes)) # inserted, key -> values
    # iv : Hash(Bytes, Array(Bytes)) # inserted, value -> keys
    #
    # pd : Set(Pos) # possibly deleted data positions
    # idx.each do |kp, vp|
    #   if {kp, vp} in d
    #     idx[{kp, vp}] = {nil, nil}
    #     pd << kp
    #     pd << vp
    #   elsif {kp, vp} in u
    #     idx[{kp, vp}] = u[{kp, vp}]
    #     pd << kp if u[{kp, vp}][0] != kp
    #     pd << vp if u[{kp, vp}][1] != vp
    #   end
    # end
    # pd.each do |p|
    #   if !ik.contains?(p) && !iv.contains?(p) && idx[{p, ...}].empty? && idx[{..., p}].empty?
    #     data[kp].free
    #   end
    # end

    def write(h : Hash(K, V?))
      kpos = Kpos.new
      @data.rewind
      lf : NamedTuple(pos: Pos, size: Size, merged: Bool)? = nil
      npos = Pos.new 0
      begin
        loop do
          cpos = Pos.new @data.pos
          b = Ph.read @data
          npos = Pos.new @data.pos

          if b.is_a? Free
            ::Log.debug { "found free block of size #{b} at #{(@data.pos - Ph.size b).to_s 16}" }
            npos = cpos + b
            if lf
              ::Log.debug { "merge" } if lf[:merged]
              lf = {pos: lf[:pos], size: lf[:size] + b, merged: true}
            else
              lf = {pos: cpos, size: b, merged: false}
            end
          else
            if lf
              @data.pos = lf[:pos]
              lfe = Pos.new lf[:pos] + lf[:size]
              kpos += write_fit lfe, h
              ::Log.debug { "write merged" } if lf[:merged]
              write_free lfe
              @data.pos = npos
              lf = nil
            end

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
              lf = {pos: (Pos.new @data.pos), size: (Size.new npos - @data.pos), merged: false} if @data.pos < npos
            end
          end

          @data.pos = npos
        end
      rescue IO::EOFError
      end
      if lf
        @data.truncate lf[:pos]
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
