require "yaml"

require "./common.cr"
require "./Log.cr"
require "./Sst.cr"

module Ph
  class Tx
    enum OpT : UInt8
      DELETE_KEY       = 0
      DELETE_VALUE     = 1
      DELETE_KEY_VALUE = 2
      INSERT           = 3
      UPDATE           = 4
    end

    alias Op = Tuple(K, Nil) |
               Tuple(Nil, V) |
               Tuple(KV, Nil) |
               KV |
               Tuple(KV, KV)

    getter ops : Array(Op) = [] of Op

    protected def initialize(@env : Env)
    end

    def delete_key(ks : Array(K))
      @ops += ks.map { |k| {k, nil} }
      self
    end

    def delete_key(k : K)
      delete_key [k]
    end

    def delete_value(vs : Array(V))
      @ops += ks.map { |v| {nil, v} }
      self
    end

    def delete_value(v : V)
      delete_value [v]
    end

    def delete(kvs : Array(KV))
      @ops += kvs.map { |kv| {kv, nil} }
      self
    end

    def delete(kv : KV)
      delete [kv]
    end

    def delete(k : K, v : V)
      delete({k, v})
    end

    def insert(kvs : Array(KV))
      @ops += kvs
      self
    end

    def insert(kv : KV)
      insert [kv]
    end

    def insert(k : K, v : V)
      insert({k, v})
    end

    def update(us : Hash(KV, KV))
      @ops += us.map { |o, n| {o, n} }
      self
    end

    def update(o : KV, n : KV)
      update({o => n})
    end

    def commit
      buf = IO::Memory.new
      @ops.each do |op|
        case op
        when Tuple(K, Nil)
          buf.write_byte OpT::DELETE_KEY.value
          Ph.write buf, op[0]
          @env.dk << op[0]
        when Tuple(Nil, V)
          buf.write_byte OpT::DELETE_VALUE.value
          Ph.write buf, op[1]
          @env.dv << op[0]
        when Tuple(KV, Nil)
          buf.write_byte OpT::DELETE_KEY_VALUE.value
          Ph.write buf, op[0]
          @env.d << op[0]
        when KV
          buf.write_byte OpT::INSERT.value
          Ph.write buf, *op
          @env.ik[op[0]] = op[1]
          @env.ik[op[1]] = op[0]
        when Tuple(KV, KV)
          buf.write_byte OpT::UPDATE.value
          Ph.write buf, *op[0]
          Ph.write buf, *op[1]
          @env.uo[op[0]] = op[1]
          @env.un[op[1]] = op[0]
        end
      end
      @env.log.write buf.to_slice
      @env
    end
  end

  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter log : Log
    getter sst : Sst

    getter dk : Set(K) = Set(K).new
    getter dv : Set(V) = Set(V).new
    getter d : Set(KV) = Set(KV).new
    getter ik : Hash(K, Array(V)) = Hash(K, Array(V)).new
    getter iv : Hash(V, Array(K)) = Hash(V, Array(K)).new
    getter uo : Hash(KV, KV) = Hash(KV, KV).new
    getter un : Hash(KV, KV) = Hash(KV, KV).new

    def after_initialize
      @log.read { |k, v| @h[k] = v }
    end

    def checkpoint
      return self if @h.empty?

      @sst.write @h
      @log.truncate
      @h.clear
      self
    end

    def tx
      Tx.new self
    end

    def get(k : K) : Array(K)
      return @h[k] rescue @sst.get k
    end
  end
end
