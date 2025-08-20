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

rnd = Random.new 1

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

  [2].each do |amount|
    it "set/get/delete for #{amount} records" do
      env = Ph::Env.from_yaml conf

      # kv = Hash(Bytes, Bytes).new
      # amount.times { kv[rnd.random_bytes(16)] = rnd.random_bytes(32) }
      kv = {("k" * 16).to_slice => ("v" * 32).to_slice,
            ("K" * 16).to_slice => ("V" * 32).to_slice}

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
