require "yaml"

module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  def self.write(io : IO, o : Bytes?)
    if o
      IO::ByteFormat::BigEndian.encode o.size.to_u16!, io
      io.write o
    else
      IO::ByteFormat::BigEndian.encode UInt16::MAX, io
    end
  end

  class Tx
    getter set : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    protected def initialize(@env : Env)
    end

    def set(kvs : Hash(K, V))
      @set.merge! kvs
      self
    end

    def set(kv : KV)
      set({kv[0] => kv[1]})
    end

    def set(k : K, v : V)
      set({k => v})
    end

    def delete(ks : Enumerable(K))
      ks.each { |k| @set[k] = nil }
      self
    end

    def delete(k : K)
      delete [k]
    end

    def commit
      buf = IO::Memory.new
      @set.each do |k, v|
        Ph.write buf, k
        Ph.write buf, v
      end
      return if buf.empty?
      @env.log.last.write buf.to_slice

      @env.h.merge! @set
    end
  end

  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    POS_SIZE = 6_i64

    getter path : String

    @[YAML::Field(ignore: true)]
    getter log : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter idx : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter data : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    def read(io : IO)
      rs = IO::ByteFormat::BigEndian.decode UInt16, io
      return nil if rs == UInt16::MAX
      r = Bytes.new rs
      io.read_fully r
      r
    end

    protected def read_log(&)
      @log.each do |_f|
        File.open _f.path do |f|
          loop do
            begin
              k = (read f).not_nil!
              v = read f
              yield({k, v})
            rescue IO::EOFError
              break
            end
          end
        end
      end
    end

    protected def filepath(i : Int32, type : String)
      ib = Bytes.new 8
      IO::ByteFormat::BigEndian.encode i.to_u64!, ib
      "#{@path}/#{type}/#{ib.hexstring}.#{type}"
    end

    def after_initialize
      Dir.mkdir_p "#{path}/log"
      Dir.mkdir_p "#{path}/idx"
      Dir.mkdir_p "#{path}/dat"

      @log = Dir.glob("#{@path}/log/*.log").sort.map { |p| File.open p, "a" }
      @log = [File.open filepath(0, "log"), "a"] if @log.empty?
      @log.each { |f| f.sync = true }

      @idx = Dir.glob("#{@path}/idx/*.idx").sort.map { |p| File.open p, "r+" }
      @idx.each { |f| f.sync = true }

      @data = Dir.glob("#{@path}/dat/*.dat").sort.map { |p| File.open p, "r+" }
      @data.each { |f| f.sync = true }

      read_log { |k, v| @h[k] = v }
    end

    def checkpoint
      return if @h.empty?

      logo = @log.pop
      @log << File.open filepath(@log.size, "log"), "a"
      @log.last.sync = true

      kvs = @h.to_a
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

      datac = File.open filepath(@data.size, "dat"), "w+"
      datac.sync = true
      datac.write datab.to_slice
      @data << datac

      idxc = File.open filepath(@idx.size, "idx"), "w+"
      idxc.sync = true
      idxc.write idxb.to_slice
      @idx << idxc

      logo.delete
      @h.clear
    end

    def tx
      Tx.new self
    end

    getter stats : Hash(String, UInt64) = {"seeks_total" => 0_u64,
                                           "seeks_short" => 0_u64,
                                           "seeks_long"  => 0_u64,
                                           "reads"       => 0_u64}

    def reset_stats
      stats.keys.each { |k| stats[k] = 0_u64 }
    end

    def get(k : Bytes)
      begin
        return @h[k]
      rescue KeyError
      end

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

            _c = k <=> (read datac).not_nil!
            @stats["reads"] += 1
            return read datac if _c == 0

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
