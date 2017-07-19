select
  n.n_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue

from
  dfs_test.tpch_customer_gd c,
  dfs_test.tpch_orders_gd o,
  dfs_test.tpch_lineitem_gd l,
  dfs_test.tpch_supplier_gd s,
  dfs_test.tpch_nation_gd n,
  dfs_test.tpch_region_gd r

where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and l.l_suppkey = s.s_suppkey
  and c.c_nationkey = s.s_nationkey
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and o.o_orderdate >= date '1997-01-01'
  and o.o_orderdate < date '1997-01-01' + interval '1' year
group by
  n.n_name

order by
  revenue desc;