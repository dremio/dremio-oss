select
  s.s_acctbal,
  s.s_name,
  n.n_name,
  p.p_partkey,
  p.p_mfgr,
  s.s_address,
  s.s_phone,
  s.s_comment
from
  dfs_test.tpch_part_gd p,
  dfs_test.tpch_supplier_gd s,
  dfs_test.tpch_partsupp_gd ps,
  dfs_test.tpch_nation_gd n,
  dfs_test.tpch_region_gd r
where
  p.p_partkey = ps.ps_partkey
  and s.s_suppkey = ps.ps_suppkey
  and p.p_size = 41
  and p.p_type like '%NICKEL'
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and ps.ps_supplycost = (

    select
      min(ps.ps_supplycost)

    from
      dfs_test.tpch_partsupp_gd ps,
      dfs_test.tpch_supplier_gd s,
      dfs_test.tpch_nation_gd n,
      dfs_test.tpch_region_gd r
    where
      p.p_partkey = ps.ps_partkey
      and s.s_suppkey = ps.ps_suppkey
      and s.s_nationkey = n.n_nationkey
      and n.n_regionkey = r.r_regionkey
      and r.r_name = 'EUROPE'
  )

order by
  s.s_acctbal desc,
  n.n_name,
  s.s_name,
  p.p_partkey
limit 100;