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

-- tpch14 using 1395599672 as a seed to the RNG
select
  100.00 * sum(case
    when p.p_type like 'PROMO%'
      then l.l_extendedprice * (1 - l.l_discount)
    else 0
  end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
  cp.`tpch/lineitem.parquet` l,
  cp.`tpch/part.parquet` p
where
  l.l_partkey = p.p_partkey
  and l.l_shipdate >= date '1994-08-01'
  and l.l_shipdate < date '1994-08-01' + interval '1' month;
