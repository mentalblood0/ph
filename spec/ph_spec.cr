require "spec"
require "../src/ph.cr"

describe Ph do
  conf = File.read "env.yml"
  env = Ph::Env.from_yaml conf

  kv = Array.new(100) { {Random::DEFAULT.random_bytes(16),
                         Random::DEFAULT.random_bytes(32)} }

  it "set/get" do
    env.set kv
    kv.each { |k, v| env.get(k).should eq v }

    env = Ph::Env.from_yaml conf
    kv.each { |k, v| env.get(k).should eq v }

    env.checkpoint
    kv.each { |k, v| env.get(k).should eq v }

    env.set "key".to_slice, "value".to_slice
    env.checkpoint
    kv.each { |k, v| env.get(k).should eq v }
    env.get("key".to_slice).should eq "value".to_slice

    env.get("nonexistent key".to_slice).should eq nil
  end
end
