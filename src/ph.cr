require "yaml"

require "./common.cr"
require "./Sst.cr"

module Ph
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

    getter path : String
    getter sst : Sst

    @[YAML::Field(ignore: true)]
    getter log : Array(File) = [] of File
    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    protected def read_log(&)
      @log.each do |_f|
        File.open _f.path do |f|
          loop do
            begin
              k = (Ph.read f).not_nil!
              v = Ph.read f
              yield({k, v})
            rescue IO::EOFError
              break
            end
          end
        end
      end
    end

    def after_initialize
      Dir.mkdir_p "#{path}/log"

      @log = Dir.glob("#{@path}/log/*.log").sort.map { |p| File.open p, "a" }
      @log = [File.open Ph.filepath(@path, 0, "log"), "a"] if @log.empty?
      @log.each { |f| f.sync = true }

      read_log { |k, v| @h[k] = v }
    end

    def checkpoint
      return if @h.empty?

      logo = @log.pop
      @log << File.open Ph.filepath(@path, @log.size, "log"), "a"
      @log.last.sync = true

      @sst.checkpoint @h

      logo.delete
      @h.clear
    end

    def tx
      Tx.new self
    end

    def get(k : Bytes)
      return @h[k] rescue @sst.get k
    end
  end
end
