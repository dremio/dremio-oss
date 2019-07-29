--
-- Copyright (C) 2017-2019 Dremio Corporation
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- tpch21 using 1395599672 as a seed to the RNG
select
  s.s_name,
  count(*) as numwait
from
  cp.`tpch/supplier.parquet` s,
  cp.`tpch/lineitem.parquet` l1,
  cp.`tpch/orders.parquet` o,
  cp.`tpch/nation.parquet` n
where
  s.s_suppkey = l1.l_suppkey
  and o.o_orderkey = l1.l_orderkey
  and o.o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
    select
      *
    from
      cp.`tpch/lineitem.parquet` l2
    where
      l2.l_orderkey = l1.l_orderkey
      and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
    select
      *
    from
      cp.`tpch/lineitem.parquet` l3
    where
      l3.l_orderkey = l1.l_orderkey
      and l3.l_suppkey <> l1.l_suppkey
      and l3.l_receiptdate > l3.l_commitdate
  )
  and s.s_nationkey = n.n_nationkey
  and n.n_name = 'BRAZIL'
group by
  s.s_name
order by
  numwait desc,
  s.s_name
limit 100;
