module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  POS_SIZE = 6_u8

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

  def self.write_size(io : IO, size : UInt64?)
    if size
      r = size + ((((effective_bits size) / 8).ceil - 1).to_u64! << 61)
      IO::ByteFormat::BigEndian.encode r, io
    else
      IO::ByteFormat::BigEndian.encode UInt16::MAX, io
    end
  end

  def self.size_size(size : UInt64)
    1 + ((IO::ByteFormat::BigEndian.decode UInt8, io) >> 5)
  end

  def self.read_size(io : IO)
    first = IO::ByteFormat::BigEndian.decode UInt8, io
    r = (first & 0b00011111).to_u64!

    other = Bytes.new first >> 5
    io.read_fully other

    other.each { |o| other = (other << 8) + o }
    r
  end

  def self.write(io : IO, o : Bytes?)
    if o
      write_size io, o.size.to_u16!
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
    return nil if rs == UInt16::MAX
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
