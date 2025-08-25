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

    ks = 16..16
    vs = 16..16
    1000.times do
      case rnd.rand 1..100
      when 1..50
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        Log.debug { "insert #{k.hexstring} #{v.hexstring}" }

        env.tx.insert(k, v).commit

        s << {k, v}
      when 51..60
        k = s.map { |k, v| k }.sample rnd rescue next
        Log.debug { "delete key #{k.hexstring}" }

        env.tx.delete_key(k).commit

        s.each { |sk, v| s.delete({k, v}) if sk == k }
      when 61..70
        v = s.map { |k, v| v }.sample rnd rescue next
        Log.debug { "delete value #{v.hexstring}" }

        env.tx.delete_value(v).commit

        s.each { |k, sv| s.delete({k, v}) if sv == v }
      when 71..80
        k, v = s.sample rnd rescue next
        Log.debug { "delete key-value #{k.hexstring} #{v.hexstring}" }

        env.tx.delete(k, v).commit

        s.delete({k, v})
      when 81..100
        k, v = s.sample rnd rescue next
        nk = rnd.random_bytes rnd.rand ks
        nv = rnd.random_bytes rnd.rand vs
        Log.debug { "update #{k.hexstring} #{v.hexstring} -> #{nk.hexstring} #{nv.hexstring}" }

        env.tx.update({k, v}, {nk, nv}).commit

        s.delete({k, v})
        s << {nk, nv}
      end
      Log.debug { "s:\n" + s.map { |k, v| "#{k.hexstring}, #{v.hexstring}" }.join '\n' }
      s.each do |k, v|
        r = env.get k
        (r.includes? v).should eq true
        r.each { |ekv| ekv[0] == k && s.includes? ekv }
      end
    end
  end
end
