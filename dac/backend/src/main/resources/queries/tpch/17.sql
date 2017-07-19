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

-- tpch17 using 1395599672 as a seed to the RNG
select
  sum(l.l_extendedprice) / 7.0 as avg_yearly
from
  cp.`tpch/lineitem.parquet` l,
  cp.`tpch/part.parquet` p
where
  p.p_partkey = l.l_partkey
  and p.p_brand = 'Brand#13'
  and p.p_container = 'JUMBO CAN'
  and l.l_quantity < (
    select
      0.2 * avg(l2.l_quantity)
    from
      cp.`tpch/lineitem.parquet` l2
    where
      l2.l_partkey = p.p_partkey
  );
