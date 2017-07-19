select
  sum(l.l_extendedprice) / 7.0 as avg_yearly
from
  dfs_test.tpch_lineitem_gd l,
  dfs_test.tpch_part_gd p
where
  p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#13'
  and p.p_container = 'JUMBO CAN'
  and l.l_quantity < (
    select
      0.2 * avg(l2.l_quantity)
    from
      dfs_test.tpch_lineitem_gd l2
    where
      l2.l_partkey = p.p_partkey
  );
