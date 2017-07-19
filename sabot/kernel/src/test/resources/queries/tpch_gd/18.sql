select
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice,
  sum(l.l_quantity)
from
  dfs_test.tpch_customer_gd c,
  dfs_test.tpch_orders_gd o,
  dfs_test.tpch_lineitem_gd l
where
  o.o_orderkey in (
    select
      l_orderkey
    from
      dfs_test.tpch_lineitem_gd
    group by
      l_orderkey having
        sum(l_quantity) > 300
  )
  and c.c_custkey = o.o_custkey
  and o.o_orderkey = l.l_orderkey
group by
  c.c_name,
  c.c_custkey,
  o.o_orderkey,
  o.o_orderdate,
  o.o_totalprice
order by
  o.o_totalprice desc,
  o.o_orderdate
limit 100;