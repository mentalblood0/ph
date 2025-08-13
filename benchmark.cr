require "benchmark"

require "./src/ph.cr"

ph = Ph::Env.from_yaml File.read "spec/config.yml"

Benchmark.ips do |b|
  b.report "set" do
    ph.set Random::DEFAULT.random_bytes(16), Random::DEFAULT.random_bytes(32)
  end
end
