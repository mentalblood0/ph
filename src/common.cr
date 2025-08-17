module Ph
  alias K = Bytes
  alias V = Bytes
  alias KV = {K, V}

  def self.write(io : IO, o : Bytes?)
    if o
      IO::ByteFormat::BigEndian.encode o.size.to_u16!, io
      io.write o
    else
      IO::ByteFormat::BigEndian.encode UInt16::MAX, io
    end
  end

  def self.read(io : IO)
    rs = IO::ByteFormat::BigEndian.decode UInt16, io
    return nil if rs == UInt16::MAX
    r = Bytes.new rs
    io.read_fully r
    r
  end

  def self.filepath(root : String, i : Int32, type : String)
    ib = Bytes.new 8
    IO::ByteFormat::BigEndian.encode i.to_u64!, ib
    "#{root}/#{type}/#{ib.hexstring}.#{type}"
  end
end
