select
  ps.ps_partkey,
  sum(ps.ps_supplycost * ps.ps_availqty) as `value`
from
  dfs_test.tpch_partsupp_gd ps,
  dfs_test.tpch_supplier_gd s,
  dfs_test.tpch_nation_gd n
where
  ps.ps_suppkey = s.s_suppkey
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'JAPAN'
group by
  ps.ps_partkey having
    sum(ps.ps_supplycost * ps.ps_availqty) > (
      select
        sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000
      from
        dfs_test.tpch_partsupp_gd ps,
        dfs_test.tpch_supplier_gd s,
        dfs_test.tpch_nation_gd n
      where
        ps.ps_suppkey = s.s_suppkey
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'JAPAN'
    )
order by
  `value` desc;