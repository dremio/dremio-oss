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

-- tpch2 using 1395599672 as a seed to the RNG
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
  cp.`tpch/part.parquet` p,
  cp.`tpch/supplier.parquet` s,
  cp.`tpch/partsupp.parquet` ps,
  cp.`tpch/nation.parquet` n,
  cp.`tpch/region.parquet` r
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
      cp.`tpch/partsupp.parquet` ps,
      cp.`tpch/supplier.parquet` s,
      cp.`tpch/nation.parquet` n,
      cp.`tpch/region.parquet` r
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
