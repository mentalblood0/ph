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
  end

  alias Op = {K, Nil} |
             {Nil, V} |
             { {K, V}, Nil } |
             {K, V}

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

        @ik[k].each { |v| @iv.delete v }
        @ik.delete k
      when {Nil, V}
        v = op[1].as V
        ::Log.debug { "register delete\n" + " " * 53 + "[*, #{v.hexstring}]" }

        @dV << v

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

        @iv[v].delete k rescue nil
        @ik[k].delete v rescue nil
      when {K, V}
        k, v = op[0].as(K), op[1].as(V)
        ::Log.debug { "register insert\n" + " " * 53 + "[#{k.hexstring}, #{v.hexstring}]" }

        @ik[k] = Set(V).new unless @ik.has_key? k
        @ik[k] << v

        @iv[v] = Set(K).new unless @iv.has_key? v
        @iv[v] << k
      else
        raise "can not commit #{op} of type #{typeof(op)}"
      end
    end

    def checkpoint
      return self if @ik.empty? && @dK.empty? && @dV.empty? && @dv.empty?

      @log.truncate
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

    def get_values(k : K) : Set(V)
      return @ik[k] rescue Set(V).new
    end

    def get_keys(v : V) : Set(K)
      return @iv[v] rescue Set(K).new
    end

    def has?(k : K, v : V) : Bool
      return (ik.has_key? k) && (ik[k].includes? v)
    end

    def check_integrity
      @ik.each { |k, vs| vs.each { |v| (@iv[v]? && @iv[v].includes? k).should eq true } }
    end
  end
end
