require "./src/ph.cr"

lib LibC
  TIOCGWINSZ = 0x5413u32

  struct Winsize
    ws_row : UInt16
    ws_col : UInt16
    ws_xpixel : UInt16
    ws_ypixel : UInt16
  end

  fun ioctl(fd : LibC::Int, what : LibC::ULong, ...) : LibC::Int
end

def getWinSize : Tuple(UInt16, UInt16)
  thing = LibC::Winsize.new
  LibC.ioctl(STDOUT.fd, LibC::TIOCGWINSZ, pointerof(thing))
  {thing.ws_row, thing.ws_col}
end

height, width = getWinSize

env_text = File.read "env.yml"
ph = Ph::Env.from_yaml env_text

conf_text = File.read "benchmark.yml"
conf = NamedTuple(amount: UInt64, key_size: UInt16, value_size: UInt16).from_yaml conf_text
bw = conf[:amount] * (2 + conf[:key_size] + 2 + conf[:value_size])

puts "-" * width
puts conf_text
puts "-" * width

kv = Hash(Bytes, Bytes).new
conf[:amount].times { kv[Random::DEFAULT.random_bytes(16)] = Random::DEFAULT.random_bytes(32) }
tw = Time.measure do
  kv.each { |k, v| ph.tx.set(k, v).commit }
end
tr = Time.measure do
  Ph::Env.from_yaml env_text
end
tc = Time.measure do
  ph.checkpoint
end
# ks = kv.keys
# ks.shuffle!
# ph.reset_stats
# tg = Time.measure do
#   ks.each { |k| ph.get k }
# end

{"write"      => tw,
 "recover"    => tr,
 "checkpoint" => tc,
 # "get"        => tg,
}.each do |o, tt|
  puts "#{o}:"
  puts "\t#{(bw / tt.total_seconds).to_u64.humanize_bytes}/s"
  puts "\t#{(conf[:amount] / tt.total_seconds).to_u64.humanize}r/s"
  puts "\t#{tt.total_seconds.humanize}s passed"
end

ph.reset_stats
ph.get kv.keys.sort.last
puts({"last key search" => ph.stats}.to_yaml)

ph.reset_stats
ph.get kv.keys.sort.first
puts({"first key search" => ph.stats}.to_yaml)

puts "#{bw}B (#{bw.humanize_bytes}) written"
puts "-" * width
