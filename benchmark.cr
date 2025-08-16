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

tw = Time.measure do
  conf[:amount].times { ph.tx.set(Random::DEFAULT.random_bytes(conf[:key_size]), Random::DEFAULT.random_bytes(conf[:value_size])).commit }
end
tr = Time.measure do
  Ph::Env.from_yaml env_text
end
tc = Time.measure do
  ph.checkpoint
end

{"write"      => tw,
 "recover"    => tr,
 "checkpoint" => tc}.each do |o, tt|
  puts "#{o}:"
  puts "\t#{(bw / tt.total_seconds).to_u64.humanize_bytes}/s"
  puts "\t#{(conf[:amount] / tt.total_seconds).to_u64.humanize}r/s"
  puts "\t#{tt.total_seconds.humanize}s passed"
end

puts "#{bw}B (#{bw.humanize_bytes}) written"
puts "-" * width
