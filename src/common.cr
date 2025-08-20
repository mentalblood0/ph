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

  def self.header_size(size : UInt32)
    ((4 + effective_bits size) / 8).ceil.to_u32!
  end

  def self.size(b : Bytes?)
    b ? b.size.to_u32! + header_size b.size.to_u32! : 1_u32
  end

  def self.size(k : Bytes, v : Bytes?)
    (size k) + (size v)
  end

  # header bits:
  # 1: allocated?
  # 1: has content?
  # 2: additional size bytes amount
  # 4: first size bits
  # 0/8/16/24: other size bits

  enum Header : UInt8
    FREE = 0b00000000_u8
    NIL  = 0b10000000_u8
    ZERO = 0b11000000_u8
  end

  alias Free = UInt32
  alias Block = Free | Nil | Bytes

  def self.write(io : IO, block : Block)
    case block
    when Nil
      io.write_byte Header::NIL.value
    when Free, Bytes
      size = (block.is_a? Free) ? block : block.size.to_u32!
      raise "too big" unless size < 2 ** 28

      obc = (header_size size) - 1
      type = (block.is_a? Free) ? Header::FREE : Header::ZERO
      r = (type.value.to_u32 << 24) | (obc << 28) | (size << ((3 - obc) * 8))

      t = Bytes.new 4
      IO::ByteFormat::BigEndian.encode r, t

      io.write t[..obc]
      io.write block if block.is_a? Bytes
    end
  end

  def self.write(io : IO, k : Bytes, v : Bytes?)
    write io, k
    write io, v
  end

  def self.read(io : IO) : Block
    first = io.read_byte.not_nil! rescue raise IO::EOFError.new
    return nil if first == Header::NIL.value

    size = (first & 0b00001111_u8).to_u32!
    other = Bytes.new (first & 0b00110000_u8) >> 4
    io.read_fully other
    other.each { |o| size = (size << 8) + o }

    return size if (first & 0b11000000) == 0

    r = Bytes.new size
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
