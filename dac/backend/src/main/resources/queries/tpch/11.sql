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

-- tpch11 using 1395599672 as a seed to the RNG
select
  ps.ps_partkey,
  sum(ps.ps_supplycost * ps.ps_availqty) as `value`
from
  cp.`tpch/partsupp.parquet` ps,
  cp.`tpch/supplier.parquet` s,
  cp.`tpch/nation.parquet` n
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
        cp.`tpch/partsupp.parquet` ps,
        cp.`tpch/supplier.parquet` s,
        cp.`tpch/nation.parquet` n
      where
        ps.ps_suppkey = s.s_suppkey
        and s.s_nationkey = n.n_nationkey
        and n.n_name = 'JAPAN'
    )
order by
  `value` desc;
