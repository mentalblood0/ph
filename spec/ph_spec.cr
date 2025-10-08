require "log"
require "spec"

require "../src/Env.cr"
require "../src/Al.cr"
require "../src/Bt.cr"
require "../src/Sds.cr"

Log.setup :debug

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

describe Ph::Sds, focus: true do
  sds = Ph::Sds.new 4_u8, 5_u8

  it "splits correctly" do
    (1_u64..1024).each do |n|
      fs = sds.fast_split n
      # s = (sds.split n).reverse
      # fs.should eq s
    end
  end
end

describe Ph::Bt do
  [2, 3, 5, 8].map { |s| s.to_u8! }.each do |s|
    it "supports #{s} bytes blocks" do
      io = IO::Memory.new
      h = Hash(Bytes, Bytes).new
      bt = Ph::Bt.new s, s, (Ph::Al.new io, s * 3), ->(k : Bytes) { h[k] }

      10.times do
        case rnd.rand 0..1
        when 0
          k = rnd.random_bytes s
          v = rnd.random_bytes 16
          h[k] = v
          bt.add k
        when 1
          k = h.keys.sample rnd rescue next
          bt.delete h[k]
          h.delete k
        end
        Log.debug { io.to_slice.hexstring.scan(/.{1,#{s * 2 * 3}}/).map(&.[0].scan(/.{1,#{s * 2}}/).join ' ').join "   " }
        h.each { |k, v| (bt.get v).should eq k }
      end
    end
  end
end

describe Ph::Al do
  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "supports #{s} bytes blocks" do
      al = Ph::Al.new IO::Memory.new, s
      l = Hash(UInt64, Bytes).new

      1000.times do
        case rnd.rand 0..2
        when 0
          b = rnd.random_bytes s
          l[al.add b] = b
        when 1
          k = l.keys.sample rnd rescue next
          al.delete k
          l.delete k
        when 2
          k = l.keys.sample rnd rescue next
          b = rnd.random_bytes s
          al.replace k, b
          l[k] = b
        end
        Log.debug { "{" + (l.map { |i, b| "#{i}: #{b.hexstring}" }.join ' ') + "}" }
        l.each { |i, b| (al.get i).should eq b }
      end
    end
  end
end

describe Ph do
  conf = File.read "env.yml"
  confp = YAML.parse conf

  Spec.before_each do
    File.delete? confp["log"]["io"]["file"]["filename"].as_s rescue nil
    # delete confp["sst"]["path"].as_s
  end

  it "writes/reads log" do
    env = Ph::Env.from_yaml conf
    log = env.log
    k = rnd.random_bytes 16
    v = rnd.random_bytes 16

    ops = [{k, nil},
           {nil, v},
           { {k, v}, nil },
           {k, v}] of Ph::Op
    log.write ops

    log.io.rewind
    log.read.should eq ops
  end

  it "generative test" do
    env = Ph::Env.from_yaml conf

    s = Set(Ph::KV).new

    ks = 16..16
    vs = 16..16
    1000.times do
      case rnd.rand 1..100
      when 1..50
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        env.tx.insert(k, v).commit
        s << {k, v}
      when 51..60
        k = s.map { |k, _| k }.sample rnd rescue next
        env.tx.delete_key(k).commit
        s.each { |sk, v| s.delete({k, v}) if sk == k }
      when 61..70
        v = s.map { |_, v| v }.sample rnd rescue next
        env.tx.delete_value(v).commit
        s.each { |k, sv| s.delete({k, v}) if sv == v }
      when 71..100
        k, v = s.sample rnd rescue next
        env.tx.delete(k, v).commit
        s.delete({k, v})
      end
      env.check_integrity
      s.each do |k, v|
        (env.has? k, v).should eq true

        rvs = env.get_values k
        (rvs.includes? v).should eq true
        rvs.each { |ekv| ekv[0] == k && s.includes? ekv }

        rks = env.get_keys v
        (rks.includes? k).should eq true
        rks.each { |ekv| ekv[0] == v && s.includes? ekv }
      end
    end
    renv = Ph::Env.from_yaml conf
    renv.ik.should eq env.ik
    renv.iv.should eq env.iv
    renv.dK.should eq env.dK
    renv.dV.should eq env.dV
    renv.dk.should eq env.dk
    renv.dv.should eq env.dv
  end
end
