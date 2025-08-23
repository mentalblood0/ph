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
    delete confp["sst"]["path"].as_s
  end

  it "matryoshka" do
    env = Ph::Env.from_yaml conf
    n = 6
    0_u8.upto(n - 1) do |i|
      k = Bytes.new 1, i
      v = Bytes.new 3 * (n - 1 - i)
      env.tx.set(k, v).commit.checkpoint
      Log.debug { "data: #{(File.read env.sst.data.path).to_slice.hexstring}" }
      env.tx.delete(k).commit.checkpoint
      Log.debug { "data: #{(File.read env.sst.data.path).to_slice.hexstring}" }
    end
    env.sst.data.size.should eq 3 * n
  end

  it "generative test" do
    env = Ph::Env.from_yaml conf

    h = Hash(Bytes, Bytes?).new

    ks = 0..1024
    vs = 0..1024
    100.times do
      Log.debug { "data: #{(File.read env.sst.data.path).to_slice.hexstring}" }
      case rnd.rand 0..2
      when 0
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        Log.debug { "add #{k.hexstring} #{v.hexstring}" }

        env.tx.set(k, v).commit

        h[k] = v
      when 1
        k = h.keys.sample rnd rescue next
        Log.debug { "delete #{k.hexstring}" }

        env.tx.delete(k).commit

        h.delete k
      when 2
        Log.debug { "checkpoint" }
        env.checkpoint
      end
      h.keys.sort.each { |k| env.get(k).should eq h[k] }
    end
  end
end
