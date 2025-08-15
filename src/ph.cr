require "yaml"

module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter path : String

    @[YAML::Field(ignore: true)]
    getter log : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter idx : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter data : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes) = Hash(Bytes, Bytes).new

    def read(io : IO)
      r = Bytes.new IO::ByteFormat::BigEndian.decode UInt16, io
      io.read r
      r
    end

    protected def read_log(&)
      @log.each do |_f|
        File.open _f.path do |f|
          loop do
            begin
              k = read f
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

    protected def write(io : IO, o : Bytes)
      IO::ByteFormat::BigEndian.encode o.size.to_u16!, io
      io.write o
    end

    def checkpoint
      logo = @log.pop
      @log << File.open filepath(@log.size, "log"), "a"
      @log.last.sync = true

      kvs = @h.to_a
      kvs.sort_by! { |k, _| k }
      idxb = IO::Memory.new
      datab = IO::Memory.new
      kvs.each do |k, v|
        IO::ByteFormat::BigEndian.encode datab.pos.to_u64!, idxb
        write datab, k
        write datab, v
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

    def set(kvs : Enumerable(KV))
      buf = IO::Memory.new
      kvs.each do |k, v|
        write buf, k
        write buf, v
      end
      @log.last.write buf.to_slice
      kvs.each { |k, v| @h[k] = v }
    end

    def set(kv : KV)
      set [kv]
    end

    def set(k : K, v : V)
      set [{k, v}]
    end

    def get(k : Bytes)
      r = @h[k]?
      return r if r

      rs = 8_i64
      (@idx.size - 1).downto(0) do |i|
        idxc = @idx[i]
        datac = @data[i]

        begin
          idxc.pos = ((idxc.size / rs).to_i64 / 2).to_i64 * rs
          step = Math.max(1_i64, idxc.pos / rs / 2)
          loop do
            datac.seek IO::ByteFormat::BigEndian.decode UInt64, idxc

            _c = k <=> read datac
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

            idxc.pos += step.to_i64 * rs - rs
          end
        rescue IO::EOFError
          next
        end
      end
    end
  end
end
