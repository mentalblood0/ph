require "spec"
require "../src/ph.cr"

def delete(path : String)
  Dir.glob("#{path}/**/*") do |file|
    File.delete(file) unless Dir.exists?(file)
  end
  Dir.glob("#{path}/**/").reverse.each do |dir|
    Dir.delete(dir)
  end
  Dir.delete(path) if Dir.exists?(path)
end

describe Ph do
  it "writes/reads size" do
    io = IO::Memory.new
    Ph.write_size io, (2 ** 8).to_u64
    puts io.to_slice
  end

  # conf = File.read "env.yml"
  # confp = YAML.parse conf

  # Spec.before_each do
  #   delete confp["log"]["path"].as_s
  #   delete confp["sst"]["path"].as_s
  # end

  # [2, 3, 10, 100, 1000].each do |amount|
  #   it "set/get/delete for #{amount} records" do
  #     env = Ph::Env.from_yaml conf

  #     kv = Hash(Bytes, Bytes).new
  #     amount.times { kv[Random::DEFAULT.random_bytes(16)] = Random::DEFAULT.random_bytes(32) }

  #     env.tx.set(kv).commit
  #     kv.each { |k, v| env.get(k).should eq v }

  #     env = Ph::Env.from_yaml conf
  #     kv.each { |k, v| env.get(k).should eq v }

  #     env.checkpoint
  #     kv.each { |k, v| env.get(k).should eq v }

  #     kn = "key".to_slice
  #     vn = "key".to_slice

  #     env.tx.set(kn, vn).commit
  #     env.checkpoint
  #     kv.each { |k, v| env.get(k).should eq v }
  #     env.get(kn).should eq vn

  #     env.get("nonexistent key".to_slice).should eq nil

  #     env.tx.delete(kv.keys.to_set).delete(kn).commit
  #     kv.each { |k, v| env.get(k).should eq nil }
  #     env.get(kn).should eq nil

  #     env.checkpoint
  #     kv.each { |k, v| env.get(k).should eq nil }
  #     env.get(kn).should eq nil

  #     env.tx.delete(kv.keys.to_set).delete(kn).commit
  #     kv.each { |k, v| env.get(k).should eq nil }
  #     env.get(kn).should eq nil

  #     env.checkpoint
  #     kv.each { |k, v| env.get(k).should eq nil }
  #     env.get(kn).should eq nil
  #   end
  # end
end
