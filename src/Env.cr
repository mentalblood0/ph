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
          ::Log.debug { "commit delete {#{k.hexstring}, *}" }

          buf.write_byte OpT::DELETE_KEY.value
          Ph.write buf, k

          @env.unk[k].as(Hash(V, {K, V})).each do |v, kv|
            @env.unv[v].delete k
            @env.uk[kv[0]].not_nil!.delete kv[1] rescue nil
            @env.uv[kv[1]].not_nil!.delete kv[0] rescue nil
          end rescue nil
          @env.unk.delete k

          @env.ik.each { |v| @env.iv.delete v }
          @env.ik.delete k
        when {Nil, V}
          v = op[1].as V
          ::Log.debug { "commit delete {*, #{v.hexstring}}" }

          buf.write_byte OpT::DELETE_VALUE.value
          Ph.write buf, v

          @env.unv[v].as(Hash(K, {K, V})).each do |k, kv|
            @env.unk[k].delete v
            @env.uk[kv[0]].not_nil!.delete kv[1] rescue nil
            @env.uv[kv[1]].not_nil!.delete kv[0] rescue nil
          end rescue nil
          @env.unv.delete v

          @env.iv.each { |k| @env.ik.delete k }
          @env.iv.delete v
        when { {K, V}, Nil }
          k = (op[0].as {K, V})[0]
          v = (op[0].as {K, V})[1]
          ::Log.debug { "commit delete {#{k.hexstring}, #{v.hexstring}}" }

          buf.write_byte OpT::DELETE_KEY_VALUE.value
          Ph.write buf, k, v
          @env.uk[k] = Hash(V, {K, V}?).new unless @env.uk.has_key? k
          @env.uk[k].not_nil![v] = nil

          @env.uv[v] = Hash(K, {K, V}?).new unless @env.uv.has_key? v
          @env.uv[v].not_nil![k] = nil

          if (@env.unk.has_key? k) && (@env.unk[k].has_key? v)
            ok, ov = @env.unk[k][v]

            @env.uk[ok] = Hash(V, {K, V}?).new unless @env.uk.has_key? ok
            @env.uk[ok].not_nil![ov] = nil
            @env.unk[k].not_nil!.delete v rescue nil

            @env.uv[ov] = Hash(K, {K, V}?).new unless @env.uv.has_key? ov
            @env.uv[ov].not_nil![ok] = nil
            @env.unv[v].not_nil!.delete k rescue nil
          end

          @env.iv[v].delete k rescue nil
          @env.ik[k].delete v rescue nil
        when {K, V}
          k = op[0].as K
          v = op[1].as V
          ::Log.debug { "commit insert {#{k.hexstring}, #{v.hexstring}}" }

          buf.write_byte OpT::INSERT.value
          Ph.write buf, k, v

          @env.ik[k] = Set(V).new unless @env.ik.has_key? k
          @env.ik[k] << v

          @env.iv[v] = Set(K).new unless @env.iv.has_key? v
          @env.iv[v] << k
        when { {K, V}, {K, V} }
          k = (op[0].as {K, V})[0]
          v = (op[0].as {K, V})[1]
          nk = (op[1].as {K, V})[0]
          nv = (op[1].as {K, V})[1]
          ::Log.debug { "commit update {#{k.hexstring}, #{v.hexstring}} -> {#{nk.hexstring}, #{nv.hexstring}}" }

          buf.write_byte OpT::UPDATE.value
          Ph.write buf, k, v
          Ph.write buf, nk, nv

          @env.uk[k] = Hash(V, {K, V}?).new unless @env.uk.has_key? k
          @env.uk[k].not_nil![v] = {nk, nv}

          @env.uv[v] = Hash(K, {K, V}?).new unless @env.uv.has_key? v
          @env.uv[v].not_nil![k] = {nk, nv}

          @env.unk[nk] = Hash(V, {K, V}).new unless @env.uk.has_key? nk
          @env.unk[nk].not_nil![nv] = {k, v}

          @env.unv[nv] = Hash(K, {K, V}).new unless @env.uv.has_key? nv
          @env.unv[nv].not_nil![nk] = {k, v}

          if (@env.unk.has_key? k) && (@env.unk[k].has_key? v)
            ok = @env.unk[k][v][0]
            ov = @env.unk[k][v][1]

            @env.unk[k].delete v
            @env.unv[v].delete k

            @env.uk[ok] = Hash(V, {K, V}?).new unless @env.uk.has_key? ok
            @env.uk[ok].not_nil![ov] = {k, v}

            @env.uv[ov] = Hash(K, {K, V}?).new unless @env.uv.has_key? ov
            @env.uv[ov].not_nil![ok] = {k, v}
          end

          if (@env.ik.has_key? k) && (@env.ik[k].includes? v)
            @env.ik[k].delete v rescue nil
            @env.ik[k] << nv rescue nil

            @env.iv[v].delete k rescue nil
            @env.iv[v] << nk rescue nil
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

    getter uk : Hash(K, Hash(V, {K, V}?)?) = Hash(K, Hash(V, {K, V}?)?).new
    getter uv : Hash(V, Hash(K, {K, V}?)?) = Hash(V, Hash(K, {K, V}?)?).new
    getter unk : Hash(K, Hash(V, Set({K, V}))) = Hash(K, Hash(V, Set({K, V}))).new
    getter unv : Hash(V, Hash(K, Set({K, V}))) = Hash(V, Hash(K, Set({K, V}))).new
    getter ik : Hash(K, Set(V)) = Hash(K, Set(V)).new
    getter iv : Hash(V, Set(K)) = Hash(V, Set(K)).new

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
            nk = nkv[0]
            nv = nkv[1]
            @unk[nk][nv].should eq({k, v})
            @unv[nv][nk].should eq({k, v})
          end
        end
      end
    end
  end
end
