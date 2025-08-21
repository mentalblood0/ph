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
  it "reads/writes values" do
    io = IO::Memory.new
    (0..16).each do |e|
      [2 ** e - 1, 2 ** e, 2 ** e + 1].each do |n|
        io.clear
        b = rnd.random_bytes n
        Ph.write io, b
        io.rewind
        (Ph.read io).should eq b
      end
    end
  end

  conf = File.read "env.yml"
  confp = YAML.parse conf

  Spec.before_each do
    File.delete? confp["log"]["path"].as_s
    delete confp["sst"]["path"].as_s
  end

  it "generative test", focus: true do
    env = Ph::Env.from_yaml conf

    h = Hash(Bytes, Bytes?).new

    100.times do
      case rnd.rand 0..2
      when 0
        k = rnd.random_bytes rnd.rand 2..16
        v = rnd.random_bytes rnd.rand 2..16
        Log.debug { "add #{k.hexstring} #{v.hexstring}" }

        env.tx.set(k, v).commit

        h[k] = v
      when 1
        k = h.keys.sample rescue next
        Log.debug { "delete #{k.hexstring}" }

        env.tx.delete(k).commit

        h.delete k
      when 2
        Log.debug { "checkpoint" }
        env.checkpoint
      end
      h.each { |k, v| env.get(k).should eq h[k] }
    end
  end

  [2, 10, 100].each do |amount|
    it "set/get/delete for #{amount} records" do
      env = Ph::Env.from_yaml conf

      kv = Hash(Bytes, Bytes).new
      amount.times { kv[rnd.random_bytes rnd.rand 2..16] = rnd.random_bytes rnd.rand 2..32 }

      env.tx.set(kv).commit
      kv.each { |k, v| env.get(k).should eq v }

      env = Ph::Env.from_yaml conf
      kv.each { |k, v| env.get(k).should eq v }

      env.checkpoint
      kv.each { |k, v| env.get(k).should eq v }

      kn = "key".to_slice
      vn = "value".to_slice

      env.tx.set(kn, vn).commit
      env.checkpoint
      kv.each { |k, v| env.get(k).should eq v }
      env.get(kn).should eq vn

      env.get("nonexistent key".to_slice).should eq nil

      env.tx.delete(kv.keys.to_set).delete(kn).commit
      kv.each { |k, v| env.get(k).should eq nil }
      env.get(kn).should eq nil

      env.checkpoint
      kv.each { |k, v| env.get(k).should eq nil }
      env.get(kn).should eq nil

      env.tx.delete(kv.keys.to_set).delete(kn).commit
      kv.each { |k, v| env.get(k).should eq nil }
      env.get(kn).should eq nil

      env.checkpoint
      kv.each { |k, v| env.get(k).should eq nil }
      env.get(kn).should eq nil
    end
  end
end
