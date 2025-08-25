require "log"
require "spec"

require "../src/Env.cr"

def delete(path : String)
  Dir.glob("#{path}/**/*") do |file|
    File.delete(file) unless Dir.exists?(file)
  end
  Dir.glob("#{path}/**/").reverse.each do |dir|
    Dir.delete(dir)
  end
  Dir.delete(path) if Dir.exists?(path)
end

rnd = Random.new 2

describe Ph do
  conf = File.read "env.yml"
  confp = YAML.parse conf

  Spec.before_each do
    File.delete? confp["log"]["path"].as_s
    # delete confp["sst"]["path"].as_s
  end

  it "generative test" do
    env = Ph::Env.from_yaml conf

    s = Set(Ph::KV).new
    hk = Hash(Ph::K, Set(Ph::V)).new
    hv = Hash(Ph::V, Set(Ph::K)).new

    ks = 16..16
    vs = 16..16
    100.times do
      case rnd.rand 0..4
      when 0
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        Log.debug { "insert #{k.hexstring} #{v.hexstring}" }

        env.tx.insert(k, v).commit

        s << {k, v}
        hk[k] = Set(Ph::V).new unless hk.has_key? k
        hk[k] << v
        hv[v] = Set(Ph::V).new unless hk.has_key? v
        hv[v] << k
      when 1
        k = hk.keys.sample rnd rescue next
        Log.debug { "delete key #{k.hexstring}" }

        env.tx.delete_key(k).commit

        hk[k].each do |v|
          s.delete({k, v})
          hv[v].delete k
        end rescue nil
        hk.delete k
      when 2
        v = hv.keys.sample rnd rescue next
        Log.debug { "delete value #{v.hexstring}" }

        env.tx.delete_value(v).commit

        hv[v].each do |k|
          s.delete({k, v})
          hk[k].delete v
        end rescue nil
        hv.delete v
      when 3
        k, v = s.sample rnd rescue next
        Log.debug { "delete key-value #{k.hexstring} #{v.hexstring}" }

        env.tx.delete(k, v).commit

        s.delete({k, v})
        hk[k].delete v
        hv[v].delete k
      when 4
        k, v = s.sample rnd rescue next
        nk = rnd.random_bytes rnd.rand ks
        nv = rnd.random_bytes rnd.rand vs
        Log.debug { "update #{k.hexstring} #{v.hexstring} -> #{nk.hexstring} #{nv.hexstring}" }

        env.tx.update({k, v}, {nk, nv}).commit

        s.delete({k, v})
        s << {nk, nv}
        hk[k].delete v
        hv[v].delete k
        hk[k] << v
        hv[v] << k
      end
      Log.debug { "\n" + s.map { |k, v| "#{k.hexstring}, #{v.hexstring}" }.join '\n' }
      hk.keys.sort.each { |k| env.get(k).should eq hk[k] }
    end
  end
end
