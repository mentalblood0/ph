require "yaml"
require "json"

require "./common"

module Ph
  class Sds
    Ph.mserializable

    getter data_size_size : UInt8
    getter pointer_size : UInt8

    getter header_size : UInt64 { @data_size_size + @pointer_size }

    def initialize(@data_size_size, @pointer_size)
    end

    def fast_split(n)
      fbi = n.trailing_zeros_count
      a = [(n.class.new 1) << fbi]
      asum = a[0]
      (fbi + 1..63).each do |i|
        if 1 == n.bit i
          b = (n.class.new 1) << i
          if a.size * @pointer_size >= b - asum
            a = [b * 2]
            asum = b * 2
          else
            a << b
            asum += b
          end
        end
      end
      a
    end
  end
end
