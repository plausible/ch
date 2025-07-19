Benchee.run(
  %{
    "old" => fn input -> Enum.each(input, &Ch.RowBinary.encode_varint/1) end,
    "new" => fn input -> Enum.each(input, &Ch.RowBinary.encode_varint_new/1) end
  },
  inputs: %{
    "small" => Enum.to_list(0..10000),
    "medium" => Enum.to_list(100_000..110_000),
    "large" => Enum.to_list(10_000_000..10_010_000)
  }
)

# ##### With input large #####
# Name           ips        average  deviation         median         99th %
# new        11.16 K       89.57 μs     ±4.27%       89.04 μs      104.63 μs
# old         4.79 K      208.97 μs     ±3.67%      207.54 μs      242.17 μs

# Comparison:
# new        11.16 K
# old         4.79 K - 2.33x slower +119.40 μs

# ##### With input medium #####
# Name           ips        average  deviation         median         99th %
# new        12.59 K       79.41 μs     ±4.36%       78.96 μs       93.58 μs
# old         6.05 K      165.30 μs     ±3.81%      164.17 μs      183.21 μs

# Comparison:
# new        12.59 K
# old         6.05 K - 2.08x slower +85.89 μs

# ##### With input small #####
# Name           ips        average  deviation         median         99th %
# new        14.53 K       68.82 μs     ±4.09%       68.50 μs       80.67 μs
# old         8.23 K      121.52 μs     ±3.67%      120.83 μs      135.03 μs

# Comparison:
# new        14.53 K
# old         8.23 K - 1.77x slower +52.70 μs
