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

-- tpch12 using 1395599672 as a seed to the RNG
select
  l.l_shipmode,
  sum(case
    when o.o_orderpriority = '1-URGENT'
      or o.o_orderpriority = '2-HIGH'
      then 1
    else 0
  end) as high_line_count,
  sum(case
    when o.o_orderpriority <> '1-URGENT'
      and o.o_orderpriority <> '2-HIGH'
      then 1
    else 0
  end) as low_line_count
from
  cp.`tpch/orders.parquet` o,
  cp.`tpch/lineitem.parquet` l
where
  o.o_orderkey = l.l_orderkey
  and l.l_shipmode in ('TRUCK', 'REG AIR')
  and l.l_commitdate < l.l_receiptdate
  and l.l_shipdate < l.l_commitdate
  and l.l_receiptdate >= date '1994-01-01'
  and l.l_receiptdate < date '1994-01-01' + interval '1' year
group by
  l.l_shipmode
order by
  l.l_shipmode;
