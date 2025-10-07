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

    # 43
    # [1, 2, 8, 32] => 4*5 + 0
    # {1, 2} => 2*5 + 0, 4 => 1*5 + 1, 10 > 6 => 4
    # {4, 8} => 2*5 + 1, 16 => 1*5 + 5, 11 > 10 => 16
    # {16, 32} => 2*5 + 5, 64 => 1*5 + 21, 15 < 26 => {16, 32}
    #
    # 75
    # [1, 2, 8, 64] => 4*5 + 0
    # {1, 2} => 2*5 + 0, 4 => 1*5 + 1, 10 > 6 => 4
    # {4, 8} => 2*5 + 1, 16 => 1*5 + 5, 11 > 10 => 16
    # {16, 64} => 2*5 + 5, 128 => 1*5 + 53, 15 < 58 => {16, 64}

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
