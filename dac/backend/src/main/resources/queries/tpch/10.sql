--
-- Copyright (C) 2017-2018 Dremio Corporation
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

-- tpch10 using 1395599672 as a seed to the RNG
select
  c.c_custkey,
  c.c_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,
  c.c_acctbal,
  n.n_name,
  c.c_address,
  c.c_phone,
  c.c_comment
from
  cp.`tpch/customer.parquet` c,
  cp.`tpch/orders.parquet` o,
  cp.`tpch/lineitem.parquet` l,
  cp.`tpch/nation.parquet` n
where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate >= date '1994-03-01'
  and o.o_orderdate < date '1994-03-01' + interval '3' month
  and l.l_returnflag = 'R'
  and c.c_nationkey = n.n_nationkey
group by
  c.c_custkey,
  c.c_name,
  c.c_acctbal,
  c.c_phone,
  n.n_name,
  c.c_address,
  c.c_comment
order by
  revenue desc
limit 20;
