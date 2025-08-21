module Ph
  alias K = Bytes
  alias V = Bytes

  alias Size = UInt32
  alias Pos = UInt64

  alias Kpos = Array(Tuple(K, Pos))
  alias Free = Size
  alias Block = Free | Nil | K | V

  POS_SIZE = 6_u8

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

  protected def self.effective_bits(value : Pos) : Size
    return Size.new 1 if value == 0
    bits = Size.new 0
    temp = Size.new value
    while temp > 0
      bits += 1
      temp >>= 1
    end
    bits
  end

  def self.header_size(size : Size) : Size
    ((4 + effective_bits size) / 8).ceil.to_u32!
  end

  def self.size(b : Block) : Size
    case b
    when Free then header_size b
    when Nil  then Size.new 1
    when K, V then Size.new b.size + header_size Size.new b.size
    else           raise "unreachable"
    end
  end

  def self.size(k : K, v : V?) : Size
    (size k) + (size v)
  end

  def self.write(io : IO, b : Block)
    case b
    when Nil
      io.write_byte Header::NIL.value
    when Free, K, V
      size = (b.is_a? Free) ? (b - header_size b) : b.size.to_u32!
      raise "too big" unless size < 2 ** 28

      obc = (header_size size) - 1
      type = (b.is_a? Free) ? Header::FREE : Header::ZERO
      r = (type.value.to_u32 << 24) | (obc << 28) | (size << ((3 - obc) * 8))

      t = Bytes.new 4
      IO::ByteFormat::BigEndian.encode r, t

      io.write t[..obc]
      io.write b if b.is_a? K | V
    end
  end

  def self.write(io : IO, k : K, v : V?)
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

  def self.write_pos(io : IO, pos : Pos)
    posb = Bytes.new 8
    IO::ByteFormat::BigEndian.encode pos, posb
    io.write posb[8 - POS_SIZE..]
  end

  def self.read_pos(io : IO) : Pos
    posb = Bytes.new 8
    io.read_fully posb[8 - POS_SIZE..]
    IO::ByteFormat::BigEndian.decode Pos, posb
  end
end
