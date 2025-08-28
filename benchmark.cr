require "log"

require "./src/Env.cr"

env_text = File.read "env.yml"
env = Ph::Env.from_yaml env_text

conf_text = File.read "benchmark.yml"
conf = NamedTuple(amount: UInt64, key_size: UInt64, value_size: UInt64).from_yaml conf_text

kv = Set(Ph::KV).new Array(Ph::KV).new conf[:amount] { {Random::DEFAULT.random_bytes(16), Random::DEFAULT.random_bytes(32)} }

tw = Time.measure do
  kv.each { |k, v| env.tx.insert(k, v).commit }
end
tr = Time.measure do
  env = Ph::Env.from_yaml env_text
end

{"write"   => tw,
 "recover" => tr,
}.each do |o, tt|
  puts "#{o}:"
  puts "\t#{(kv.size / tt.total_seconds).to_u64.humanize}r/s"
  puts "\t#{tt.total_seconds.humanize}s passed"
end
