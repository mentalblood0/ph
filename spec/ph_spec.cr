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

    ks = 0..1024
    vs = 0..1024
    100.times do
      case rnd.rand 0..2
      when 0
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        Log.debug { "insert #{k.hexstring} #{v.hexstring}" }

        env.tx.insert(k, v).commit

        s << {k, v}
        hk[k] = Set(Ph::V).new unless hk.has_key? k
        hk[k] << v
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
      end
      hk.keys.sort.each { |k| env.get(k).should eq hk[k] }
    end
  end
end
