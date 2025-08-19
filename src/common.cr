module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  POS_SIZE = 6_u8
  SIZE_NIL = UInt64::MAX >> 3

  protected def self.effective_bits(value : UInt64) : Int32
    return 1 if value == 0
    bits = 0
    temp = value
    while temp > 0
      bits += 1
      temp >>= 1
    end
    bits
  end

  def self.size(size : UInt64)
    ((3 + effective_bits size) / 8).ceil.to_u64!
  end

  def self.size(b : Bytes)
    b.size.to_u64! + size b.size.to_u64!
  end

  def self.size(k : Bytes, v : Bytes)
    (size k) + (size v)
  end

  def self.write_size(io : IO, size : UInt64?)
    if size
      obc = (size size) - 1
      puts "write size #{size}; obc = #{obc}"
      r = (size << ((7 - obc) * 8)) + (obc << 61)
      t = Bytes.new 8
      IO::ByteFormat::BigEndian.encode r, t
      io.write t[..obc]
    else
      write_size io, SIZE_NIL
    end
  end

  def self.read_size(io : IO)
    puts "read_size from pos #{io.pos}"
    first = IO::ByteFormat::BigEndian.decode UInt8, io
    puts first
    r = (first & 0b00011111).to_u64!

    other = Bytes.new first >> 5
    io.read_fully other

    other.each { |o| r = (r << 8) + o }
    r
  end

  def self.write(io : IO, o : Bytes?)
    if o
      write_size io, o.size.to_u64!
      io.write o
    else
      write_size io, nil
    end
  end

  def self.write(io : IO, k : Bytes, v : Bytes?)
    write io, k
    write io, v
  end

  def self.read(io : IO)
    rs = read_size io
    return nil if rs == SIZE_NIL
    r = Bytes.new rs
    io.read_fully r
    r
  end

  def self.write_pos(io : IO, pos : UInt64)
    posb = Bytes.new 8
    IO::ByteFormat::BigEndian.encode pos, posb
    io.write posb[8 - POS_SIZE..]
  end

  def self.read_pos(io : IO)
    posb = Bytes.new 8
    io.read_fully posb[8 - POS_SIZE..]
    IO::ByteFormat::BigEndian.decode UInt64, posb
  end

  def self.filepath(root : String, i : Int32, type : String)
    ib = Bytes.new 8
    IO::ByteFormat::BigEndian.encode i.to_u64!, ib
    "#{root}/#{ib.hexstring}.#{type}"
  end
end
