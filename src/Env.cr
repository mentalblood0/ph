require "spec"
require "yaml"

require "./common.cr"
require "./Log.cr"

module Ph
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

  class Tx
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
      @env.log.write @ops
      @ops.each { |op| @env.register op }
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
      ::Log.debug { "recover" }
      log.read { |op| register op }
    end

    def register(op : Op)
      case op
      when {K, Nil}
        k = op[0].as K
        ::Log.debug { "register delete\n" + " " * 53 + "[#{k.hexstring}, *]" }

        @dK << k

        @unk[k].each do |v, kvs|
          @unv[v].delete k
          kvs.each do |kv|
            @uk[kv[0]].not_nil!.delete kv[1]
            @uv[kv[1]].not_nil!.delete kv[0]
          end
        end rescue nil
        @unk.delete k

        @ik[k].each { |v| @iv.delete v }
        @ik.delete k
      when {Nil, V}
        v = op[1].as V
        ::Log.debug { "register delete\n" + " " * 53 + "[*, #{v.hexstring}]" }

        @dV << v

        @unv[v].each do |k, kvs|
          @unk[k].delete v
          kvs.each do |kv|
            @uk[kv[0]].not_nil!.delete kv[1]
            @uv[kv[1]].not_nil!.delete kv[0]
          end
        end rescue nil
        @unv.delete v

        @iv[v].each { |k| @ik.delete k }
        @iv.delete v
      when { {K, V}, Nil }
        k, v = op[0].as {K, V}
        ::Log.debug { "register delete\n" + " " * 53 + "[#{k.hexstring}, #{v.hexstring}]" }

        unless (@dK.includes? k) || (@dV.includes? v)
          @dk[k] = Set(V).new unless @dk.has_key? k
          @dk[k] << v

          @dv[v] = Set(K).new unless @dv.has_key? v
          @dv[v] << k
        end

        @uk[k] = Hash(V, {K, V}?).new unless @uk.has_key? k
        @uk[k].not_nil![v] = nil

        @uv[v] = Hash(K, {K, V}?).new unless @uv.has_key? v
        @uv[v].not_nil![k] = nil

        if (@unk.has_key? k) && (@unk[k].has_key? v)
          @unk[k][v].each do |ok, ov|
            @uk[ok] = Hash(V, {K, V}?).new unless @uk.has_key? ok
            @uk[ok].not_nil![ov] = nil
            @unk[k].not_nil!.delete v rescue nil

            @uv[ov] = Hash(K, {K, V}?).new unless @uv.has_key? ov
            @uv[ov].not_nil![ok] = nil
            @unv[v].not_nil!.delete k rescue nil
          end
        end

        @iv[v].delete k rescue nil
        @ik[k].delete v rescue nil
      when {K, V}
        k, v = op[0].as(K), op[1].as(V)
        ::Log.debug { "register insert\n" + " " * 53 + "[#{k.hexstring}, #{v.hexstring}]" }

        @ik[k] = Set(V).new unless @ik.has_key? k
        @ik[k] << v

        @iv[v] = Set(K).new unless @iv.has_key? v
        @iv[v] << k
      when { {K, V}, {K, V} }
        k, v = op[0].as {K, V}
        nk, nv = op[1].as {K, V}
        ::Log.debug { "register update\n" + " " * 53 + "[#{k.hexstring}, #{v.hexstring}] ->\n" + " " * 53 + "[#{nk.hexstring}, #{nv.hexstring}]" }

        @uk[k] = Hash(V, {K, V}?).new unless @uk.has_key? k
        @uk[k].not_nil![v] = {nk, nv}

        @uv[v] = Hash(K, {K, V}?).new unless @uv.has_key? v
        @uv[v].not_nil![k] = {nk, nv}

        @unk[nk] = Hash(V, Set({K, V})).new unless @unk.has_key? nk
        @unk[nk].not_nil![nv] = Set({K, V}).new unless @unk[nk].has_key? nv
        @unk[nk].not_nil![nv] << {k, v}

        @unv[nv] = Hash(K, Set({K, V})).new unless @unv.has_key? nv
        @unv[nv].not_nil![nk] = Set({K, V}).new unless @unv[nv].has_key? nk
        @unv[nv].not_nil![nk] << {k, v}

        if (@unk.has_key? k) && (@unk[k].has_key? v)
          @unk[k][v].each do |ok, ov|
            @uk[ok].not_nil![ov] = {nk, nv}
            @uv[ov].not_nil![ok] = {nk, nv}
            @unk[nk][nv] << {ok, ov}
            @unv[nv][nk] << {ok, ov}
          end
          @unk[k].delete v
          @unv[v].delete k
        end

        if (@ik.has_key? k) && (@ik[k].includes? v)
          @ik[k].delete v
          @ik[nk] = Set(V).new unless @ik.has_key? nk
          @ik[nk] << nv

          @iv[v].delete k
          @iv[nv] = Set(K).new unless @iv.has_key? nv
          @iv[nv] << nk
        end
      else
        raise "can not commit #{op} of type #{typeof(op)}"
      end
    end

    def checkpoint
      return self if @uk.empty? && @ik.empty? && @dK.empty? && @dV.empty? && @dv.empty?

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
