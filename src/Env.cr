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

          @env.unk[k].as(Hash(V, KV?)).each do |v, kv|
            @env.unv[v].delete k
            @env.uk[kv[0]].delete kv[1]
            @env.uv[kv[1]].delete kv[0]
          end rescue nil
          @env.unk[k] = nil

          @env.ik.each { |v| iv.delete v }
          @env.ik.delete k
        when Tuple(Nil, V)
          buf.write_byte OpT::DELETE_VALUE.value
          Ph.write buf, op[1]

          @env.unv[v].as(Hash(K, KV?)).each do |k, kv|
            @env.unk[k].delete v
            @env.uk[kv[0]].delete kv[1]
            @env.uv[kv[1]].delete kv[0]
          end rescue nil
          @env.unv[v] = nil

          @env.iv.each { |k| ik.delete k }
          @env.iv.delete v
        when Tuple(KV, Nil)
          k = op[0][0]
          v = op[0][1]
          buf.write_byte OpT::DELETE_KEY_VALUE.value
          Ph.write buf, k, v
          @env.uk[k] = Hash(V, KV?).new unless @env.uk.has_key? k
          @env.uk[k][v] = nil

          @env.uv[v] = Hash(K, KV?).new unless @env.uv.has_key? v
          @env.uv[v][k] = nil

          @env.unk[k][v].each do |ok, ov|
            @env.uk[ok] = Hash(V, KV?).new unless @env.uk.has_key? ok
            @env.uk[ok][ov] = nil
          end rescue nil
          @env.unk[k].delete v rescue nil

          @env.unv[v][k].each do |ok, ov|
            @env.uv[ov] = Hash(K, KV?).new unless @env.uv.has_key? ov
            @env.uv[ov][ok] = nil
          end rescue nil
          @env.unv[v].delete k rescue nil

          @env.iv[v].delete k rescue nil
          @env.ik[k].delete v rescue nil
        when KV
          buf.write_byte OpT::INSERT.value
          Ph.write buf, *op

          @env.ik[k] = Set(V).new unless @env.ik.has_key? k
          @env.ik[k] << v

          @env.iv[v] = Set(K).new unless @env.iv.has_key? v
          @env.iv[v] << k
        when Tuple(KV, KV)
          k = op[0][0]
          v = op[0][1]
          nk = op[1][0]
          nv = op[1][1]
          buf.write_byte OpT::UPDATE.value
          Ph.write buf, *op[0]
          Ph.write buf, *op[1]

          @env.uk[k] = Hash(V, KV?).new unless @env.uk.has_key? k
          @env.uk[k][v] = {nk, nv}

          @env.uv[v] = Hash(K, KV?).new unless @env.uv.has_key? v
          @env.uv[v][k] = {nv, nk}

          if (@env.unk.has_key? k) && (@env.unk[k].has_key? v)
            ok = @env.unk[k][v][0]
            ov = @env.unk[k][v][1]

            @env.unk[k].delete v
            @env.unv[v].delete k

            @env.uk[ok] = Hash(V, KV?).new unless @env.uk.has_key? ok
            @env.uk[ok][ov] = {k, v}

            @env.uv[ov] = Hash(K, KV?).new unless @env.uv.has_key? ov
            @env.uv[ov][ok] = {v, k}
          end

          if (@env.ik.has_key? k) && (@env.ik[k].includes? v)
            @env.ik[k].delete v rescue nil
            @env.ik[k] << v rescue nil

            @env.iv[v].delete k rescue nil
            @env.iv[v] << k rescue nil
          end
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

    getter uk : Hash(K, Hash(V, KV?)?) = Hash(K, Hash(V, KV?)?).new
    getter uv : Hash(V, Hash(K, KV?)?) = Hash(V, Hash(K, KV?)?).new
    getter unk : Hash(K, Hash(V, KV?)?) = Hash(K, Hash(V, KV?)?).new
    getter unv : Hash(V, Hash(K, KV?)?) = Hash(V, Hash(K, KV?)?).new
    getter ik : Hash(K, Set(V)) = Hash(K, Set(V)).new
    getter iv : Hash(V, Set(K)) = Hash(V, Set(K)).new

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
