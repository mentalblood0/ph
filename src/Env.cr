require "yaml"

require "./common.cr"
require "./Log.cr"
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
      @env.log.write buf.to_slice

      @env.h.merge! @set
    end
  end

  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter log : Log
    getter sst : Sst

    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    def after_initialize
      @log.read { |k, v| @h[k] = v }
    end

    def checkpoint
      return if @h.empty?

      @sst.write @h
      @log.truncate
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
