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

-- tpch16 using 1395599672 as a seed to the RNG
select
  p.p_brand,
  p.p_type,
  p.p_size,
  count(distinct ps.ps_suppkey) as supplier_cnt
from
  cp.`tpch/partsupp.parquet` ps,
  cp.`tpch/part.parquet` p
where
  p.p_partkey = ps.ps_partkey
  and p.p_brand <> 'Brand#21'
  and p.p_type not like 'MEDIUM PLATED%'
  and p.p_size in (38, 2, 8, 31, 44, 5, 14, 24)
  and ps.ps_suppkey not in (
    select
      s.s_suppkey
    from
      cp.`tpch/supplier.parquet` s
    where
      s.s_comment like '%Customer%Complaints%'
  )
group by
  p.p_brand,
  p.p_type,
  p.p_size
order by
  supplier_cnt desc,
  p.p_brand,
  p.p_type,
  p.p_size;
