require "yaml"
require "json"

require "./common"

module Ph
  class Sds
    Ph.mserializable

    getter dss : UInt8
    getter ps : UInt8

    getter hs : UInt64 { @dss + @ps }

    def initialize(@dss, @ps)
    end

    def fast_split(n : UInt64)
      fbi = n.trailing_zeros_count
      a = [1_u64 << fbi]
      asum = a[0]
      (fbi + 1..63).each do |i|
        if 1 == n.bit i
          b = 1_u64 << i
          if @ps * a.size >= b - asum
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

    def split(n : UInt64)
      dp = Array(UInt64).new(n + 1, UInt64::MAX)
      dp[0] = 0_u64

      used = Array(Array(UInt64)).new(n + 1) { [] of UInt64 }

      pow = (Math.log n, 2).ceil.to_i32
      while pow >= 0
        b = 1_u64 << pow
        (n).downto(0) do |c|
          next if dp[c] == UInt64::MAX

          max_possible = {c + b, n}.min

          (c + 1..max_possible).each do |new|
            overhead = if new <= b
                         b - new
                       else
                         b - (new - c)
                       end
            overhead = 0 if overhead < 0

            total_cost = dp[c] + overhead + 5

            if total_cost < dp[new]
              dp[new] = total_cost
              used[new] = used[c] + [b]
            end
          end
        end
        pow -= 1
      end

      used[n]
    end

    def body_cost(s : Array(UInt64))
      (((s.size == 1) ? 0 : s.size) * @ps + # blocks pointers
        s.sum).to_u64!                      # blocks
    end

    def cost(s : Array(UInt64)) : UInt64
      hs + body_cost s
    end
  end
end
