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

-- tpch20 using 1395599672 as a seed to the RNG
select
  s.s_name,
  s.s_address
from
  cp.`tpch/supplier.parquet` s,
  cp.`tpch/nation.parquet` n
where
  s.s_suppkey in (
    select
      ps.ps_suppkey
    from
      cp.`tpch/partsupp.parquet` ps
    where
      ps. ps_partkey in (
        select
          p.p_partkey
        from
          cp.`tpch/part.parquet` p
        where
          p.p_name like 'antique%'
      )
      and ps.ps_availqty > (
        select
          0.5 * sum(l.l_quantity)
        from
          cp.`tpch/lineitem.parquet` l
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
