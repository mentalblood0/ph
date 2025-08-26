require "spec"
require "yaml"

require "./common.cr"
require "./Log.cr"

module Ph
  class Tx
    enum OpT : UInt8
      DELETE_KEY       = 0
      DELETE_VALUE     = 1
      DELETE_KEY_VALUE = 2
      INSERT           = 3
      UPDATE           = 4
    end

    alias Op = {K, Nil} |
               {Nil, V} |
               { {K, V}, Nil } |
               {K, V} |
               { {K, V}, {K, V} }

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
      @ops += vs.map { |v| {nil, v} }
      self
    end

    def delete_value(v : V)
      delete_value [v]
    end

    def delete(kvs : Array({K, V}))
      @ops += kvs.map { |kv| {kv, nil} }
      self
    end

    def delete(k : K, v : V)
      delete([{k, v}])
    end

    def insert(kvs : Array({K, V}))
      @ops += kvs
      self
    end

    def insert(k : K, v : V)
      insert [{k, v}]
    end

    def update(us : Hash({K, V}, {K, V}))
      @ops += us.map { |o, n| {o, n} }
      self
    end

    def update(o : {K, V}, n : {K, V})
      update({o => n})
    end

    def commit
      buf = IO::Memory.new
      @ops.each do |op|
        case op
        when {K, Nil}
          k = op[0].as K
          ::Log.debug { "commit delete\n" + " " * 37 + "[#{k.hexstring}, *]" }

          buf.write_byte OpT::DELETE_KEY.value
          Ph.write buf, k

          @env.dK << k

          @env.unk[k].each do |v, kvs|
            @env.unv[v].delete k
            kvs.each do |kv|
              @env.uk[kv[0]].not_nil!.delete kv[1]
              @env.uv[kv[1]].not_nil!.delete kv[0]
            end
          end rescue nil
          @env.unk.delete k

          @env.ik[k].each { |v| @env.iv.delete v }
          @env.ik.delete k
        when {Nil, V}
          v = op[1].as V
          ::Log.debug { "commit delete\n" + " " * 37 + "[*, #{v.hexstring}]" }

          buf.write_byte OpT::DELETE_VALUE.value
          Ph.write buf, v

          @env.dV << v

          @env.unv[v].each do |k, kvs|
            @env.unk[k].delete v
            kvs.each do |kv|
              @env.uk[kv[0]].not_nil!.delete kv[1]
              @env.uv[kv[1]].not_nil!.delete kv[0]
            end
          end rescue nil
          @env.unv.delete v

          @env.iv[v].each { |k| @env.ik.delete k }
          @env.iv.delete v
        when { {K, V}, Nil }
          k, v = op[0].as {K, V}
          ::Log.debug { "commit delete\n" + " " * 37 + "[#{k.hexstring}, #{v.hexstring}]" }

          @env.dk[k] = Set(V).new unless @env.dk.has_key? k
          @env.dk[k] << v

          @env.dv[v] = Set(K).new unless @env.dv.has_key? v
          @env.dv[v] << k

          buf.write_byte OpT::DELETE_KEY_VALUE.value
          Ph.write buf, k, v
          @env.uk[k] = Hash(V, {K, V}?).new unless @env.uk.has_key? k
          @env.uk[k].not_nil![v] = nil

          @env.uv[v] = Hash(K, {K, V}?).new unless @env.uv.has_key? v
          @env.uv[v].not_nil![k] = nil

          if (@env.unk.has_key? k) && (@env.unk[k].has_key? v)
            @env.unk[k][v].each do |ok, ov|
              @env.uk[ok] = Hash(V, {K, V}?).new unless @env.uk.has_key? ok
              @env.uk[ok].not_nil![ov] = nil
              @env.unk[k].not_nil!.delete v rescue nil

              @env.uv[ov] = Hash(K, {K, V}?).new unless @env.uv.has_key? ov
              @env.uv[ov].not_nil![ok] = nil
              @env.unv[v].not_nil!.delete k rescue nil
            end
          end

          @env.iv[v].delete k rescue nil
          @env.ik[k].delete v rescue nil
        when {K, V}
          k, v = op[0].as(K), op[1].as(V)
          ::Log.debug { "commit insert\n" + " " * 37 + "[#{k.hexstring}, #{v.hexstring}]" }

          buf.write_byte OpT::INSERT.value
          Ph.write buf, k, v

          @env.ik[k] = Set(V).new unless @env.ik.has_key? k
          @env.ik[k] << v

          @env.iv[v] = Set(K).new unless @env.iv.has_key? v
          @env.iv[v] << k
        when { {K, V}, {K, V} }
          k, v = op[0].as {K, V}
          nk, nv = op[1].as {K, V}
          ::Log.debug { "commit update\n" + " " * 37 + "[#{k.hexstring}, #{v.hexstring}] ->\n" + " " * 37 + "[#{nk.hexstring}, #{nv.hexstring}]" }

          buf.write_byte OpT::UPDATE.value
          Ph.write buf, k, v
          Ph.write buf, nk, nv

          @env.uk[k] = Hash(V, {K, V}?).new unless @env.uk.has_key? k
          @env.uk[k].not_nil![v] = {nk, nv}

          @env.uv[v] = Hash(K, {K, V}?).new unless @env.uv.has_key? v
          @env.uv[v].not_nil![k] = {nk, nv}

          @env.unk[nk] = Hash(V, Set({K, V})).new unless @env.unk.has_key? nk
          @env.unk[nk].not_nil![nv] = Set({K, V}).new unless @env.unk[nk].has_key? nv
          @env.unk[nk].not_nil![nv] << {k, v}

          @env.unv[nv] = Hash(K, Set({K, V})).new unless @env.unv.has_key? nv
          @env.unv[nv].not_nil![nk] = Set({K, V}).new unless @env.unv[nv].has_key? nk
          @env.unv[nv].not_nil![nk] << {k, v}

          if (@env.unk.has_key? k) && (@env.unk[k].has_key? v)
            @env.unk[k][v].each do |ok, ov|
              @env.uk[ok].not_nil![ov] = {nk, nv}
              @env.uv[ov].not_nil![ok] = {nk, nv}
              @env.unk[nk][nv] << {ok, ov}
              @env.unv[nv][nk] << {ok, ov}
            end
            @env.unk[k].delete v
            @env.unv[v].delete k
          end

          if (@env.ik.has_key? k) && (@env.ik[k].includes? v)
            @env.ik[k].delete v
            @env.ik[nk] = Set(V).new unless @env.ik.has_key? nk
            @env.ik[nk] << nv

            @env.iv[v].delete k
            @env.iv[nv] = Set(K).new unless @env.iv.has_key? nv
            @env.iv[nv] << nk
          end
        else
          raise "can not commit #{op} of type #{typeof(op)}"
        end
      end
      ::Log.debug { "dump transaction to log" }
      @env.log.write buf.to_slice
      @env
    end
  end

  class Env
    include YAML::Serializable
    include YAML::Serializable::Strict

    getter log : Log

    getter uk = Hash(K, Hash(V, {K, V}?)?).new
    getter uv = Hash(V, Hash(K, {K, V}?)?).new
    getter unk = Hash(K, Hash(V, Set({K, V}))).new
    getter unv = Hash(V, Hash(K, Set({K, V}))).new
    getter ik = Hash(K, Set(V)).new
    getter iv = Hash(V, Set(K)).new
    getter dK = Set(K).new
    getter dV = Set(V).new
    getter dk = Hash(K, Set(V)).new
    getter dv = Hash(V, Set(K)).new

    def after_initialize
    end

    def checkpoint
      return self if @uk.empty? && @ik.empty?

      @log.truncate
      @uk.clear
      @uv.clear
      @unk.clear
      @unv.clear
      @ik.clear
      @iv.clear
      @dK.clear
      @dV.clear
      @dk.clear
      @dv.clear
      self
    end

    def tx
      Tx.new self
    end

    def get(k : K) : Set(V)
      return (Set.new @unk[k].keys) rescue @ik[k] rescue Set(V).new
    end

    def has?(k : K, v : V) : Bool
      return ((unk.has_key? k) && (unk[k].has_key? v)) || ((ik.has_key? k) && (ik[k].includes? v))
    end

    def check_integrity
      @uk.each do |k, vkv|
        next unless vkv
        vkv.each do |v, nkv|
          @uv[v].not_nil![k].should eq nkv
          if nkv
            nk, nv = nkv
            @unk[nk][nv].includes?({k, v}).should eq true
            @unv[nv][nk].includes?({k, v}).should eq true
          end
        end
      end
      @ik.each { |k, vs| vs.each { |v| (@iv[v]? && @iv[v].includes? k).should eq true } }
    end
  end
end
