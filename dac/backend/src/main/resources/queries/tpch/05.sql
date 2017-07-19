--
-- Copyright (C) 2017 Dremio Corporation
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

-- tpch5 using 1395599672 as a seed to the RNG
select
  n.n_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue

from
  cp.`tpch/customer.parquet` c,
  cp.`tpch/orders.parquet` o,
  cp.`tpch/lineitem.parquet` l,
  cp.`tpch/supplier.parquet` s,
  cp.`tpch/nation.parquet` n,
  cp.`tpch/region.parquet` r

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