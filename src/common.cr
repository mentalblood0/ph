module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  POS_SIZE = 6_u8

  def self.write_size(io : IO, size : UInt16?)
    if size
      IO::ByteFormat::BigEndian.encode size, io
    else
      IO::ByteFormat::BigEndian.encode UInt16.MAX, io
    end
  end

  def self.write(io : IO, o : Bytes?)
    if o
      write_size io, o.size.to_u16!
      io.write o
    else
      write_size io, nil
    end
  end

  def self.read(io : IO)
    rs = IO::ByteFormat::BigEndian.decode UInt16, io
    return nil if rs == UInt16::MAX
    r = Bytes.new rs
    io.read_fully r
    r
  end

  def self.write_pos(io : IO, pos : UInt64)
    posb = Bytes.new 8
    IO::ByteFormat::BigEndian.encode pos, posb
    idxb.write posb[8 - POS_SIZE..]
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
