select
  o.o_orderpriority,
  count(*) as order_count
from
  dfs_test.tpch_orders_gd o

where
  o.o_orderdate >= date '1996-10-01'
  and o.o_orderdate < date '1996-10-01' + interval '3' month
  and
  exists (
    select
      *
    from
      dfs_test.tpch_lineitem_gd l
    where
      l.l_orderkey = o.o_orderkey
      and l.l_commitdate < l.l_receiptdate
  )
group by
  o.o_orderpriority
order by
  o.o_orderpriority;
