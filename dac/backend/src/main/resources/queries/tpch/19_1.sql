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

-- tpch19 using 1395599672 as a seed to the RNG
select
  sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
  cp.`tpch/lineitem.parquet` l,
  cp.`tpch/part.parquet` p
where
    p.p_partkey = l.l_partkey
    and (
  (
    p.p_brand = 'Brand#41'
    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and l.l_quantity >= 2 and l.l_quantity <= 2 + 10
    and p.p_size between 1 and 5
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_brand = 'Brand#13'
    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and l.l_quantity >= 14 and l.l_quantity <= 14 + 10
    and p.p_size between 1 and 10
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  )
  or
  (
    p.p_brand = 'Brand#55'
    and p.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and l.l_quantity >= 23 and l.l_quantity <= 23 + 10
    and p.p_size between 1 and 15
    and l.l_shipmode in ('AIR', 'AIR REG')
    and l.l_shipinstruct = 'DELIVER IN PERSON'
  ) );
