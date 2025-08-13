require "spec"
require "../src/ph.cr"

describe Ph do
  conf = File.read "env.yml"
  env = Ph::Env.from_yaml conf

  k = "key".to_slice
  v = "value".to_slice

  it "set" do
    env.set k, v
    env.get(k).should eq v
  end

  it "recover" do
    env.set k, v
    Ph::Env.from_yaml(conf).get(k).should eq v
  end
end
