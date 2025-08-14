require "spec"
require "../src/ph.cr"

describe Ph do
  conf = File.read "env.yml"
  env = Ph::Env.from_yaml conf

  kv = Array.new(100) { {Random::DEFAULT.random_bytes(16),
                         Random::DEFAULT.random_bytes(32)} }

  it "set/get" do
    kv.each { |k, v| env.set k, v }
    env = Ph::Env.from_yaml conf
    kv.each { |k, v| env.get(k).should eq v }
    env.checkpoint
    kv.each { |k, v| env.get(k).should eq v }
  end
end
