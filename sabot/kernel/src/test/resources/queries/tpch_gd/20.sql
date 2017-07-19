select
  s.s_name,
  s.s_address
from
  dfs_test.tpch_supplier_gd s,
  dfs_test.tpch_nation_gd n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      dfs_test.tpch_partsupp_gd ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          dfs_test.tpch_part_gd p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          dfs_test.tpch_lineitem_gd l
        where
          l.l_partkey = ps.ps_partkey
          and l.l_suppkey = ps.ps_suppkey
          and l.l_shipdate >= date '1993-01-01'
          and l.l_shipdate < date '1993-01-01' + interval '1' year
      )
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'KENYA'
order by
  s.s_name;