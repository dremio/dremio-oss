/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.sabot.aggregate.hash;

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.op.aggregate.hash.HashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;

public class TestHashAgg extends BaseTestOperator {

  @Test
  public void oneKeySumCnt() throws Exception {
    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("r_name")),
        Arrays.asList(
            n("sum(r_regionkey)", "sum"),
            n("count(r_regionkey)", "cnt")
            ),
        false,
        1f);

    final Table expected = t(
        th("r_name",    "sum", "cnt"),
        tr("AFRICA",      0L, 1L),
        tr("AMERICA",     1L, 1L),
        tr("ASIA",        2L, 1L),
        tr("EUROPE",      3L, 1L),
        tr("MIDDLE EAST", 4L, 1L)
        );

    validateSingle(conf, HashAggOperator.class, TpchTable.REGION, 0.1, expected);
  }


  @Test
  public void oneKeySumCntVectorized() throws Exception {
    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("r_name")),
        Arrays.asList(
            n("sum(r_regionkey)", "sum"),
            n("count(r_regionkey)", "cnt")
            ),
        true,
        1f);

    final Table expected = t(
        th("r_name",    "sum", "cnt"),
        tr("AFRICA",      0L, 1L),
        tr("AMERICA",     1L, 1L),
        tr("ASIA",        2L, 1L),
        tr("EUROPE",      3L, 1L),
        tr("MIDDLE EAST", 4L, 1L)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, TpchTable.REGION, 0.1, expected);
  }

  @Test
  public void intWork() throws Exception {

    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("gb")),
        Arrays.asList(
            n("sum(myint)", "sum"),
            n("count(myint)", "cnt"),
            n("$sum0(myint)", "sum0"),
            n("min(myint)", "min"),
            n("max(myint)", "max")
            ),
        true,
        1f);

    final Table expected = t(
        th("gb",    "sum", "cnt", "sum0", "min", "max"),
        tr("group1",     -5L, 2L, -5l, -10, 5),
        tr("group2",     -3L, 2L, -3l, -13, 10),
        tr("group3",     Fixtures.NULL_BIGINT, 0L, 0l, Fixtures.NULL_INT, Fixtures.NULL_INT)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, DATA, expected);
  }

  @Test
  public void bigintWork() throws Exception {

    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("gb")),
        Arrays.asList(
            n("sum(mybigint)", "sum"),
            n("count(mybigint)", "cnt"),
            n("$sum0(mybigint)", "sum0"),
            n("min(mybigint)", "min"),
            n("max(mybigint)", "max")
            ),
        true,
        1f);

    final Table expected = t(
        th("gb",    "sum", "cnt", "sum0", "min", "max"),
        tr("group1",     -5L, 2L, -5l, -10l, 5l),
        tr("group2",     -3L, 2L, -3l, -13l, 10l),
        tr("group3",     Fixtures.NULL_BIGINT, 0L, 0l, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, DATA, expected);
  }

  @Test
  public void floatWork() throws Exception {

    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("gb")),
        Arrays.asList(
            n("sum(myfloat)", "sum"),
            n("count(myfloat)", "cnt"),
            n("$sum0(myfloat)", "sum0"),
            n("min(myfloat)", "min"),
            n("max(myfloat)", "max")
            ),
        true,
        1f);

    final Table expected = t(
        th("gb",    "sum", "cnt", "sum0", "min", "max"),
        tr("group1",     -5.0D, 2L, -5.0D, -10.0f, 5.0f),
        tr("group2",     -3.0D, 2L, -3.0D, -13.0f, 10.0f),
        tr("group3",     Fixtures.NULL_DOUBLE, 0L, 0D, Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, DATA, expected);
  }


  @Test
  public void doubleWork() throws Exception {

    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("gb")),
        Arrays.asList(
            n("sum(mydouble)", "sum"),
            n("count(mydouble)", "cnt"),
            n("$sum0(mydouble)", "sum0"),
            n("min(mydouble)", "min"),
            n("max(mydouble)", "max")
            ),
        true,
        1f);

    final Table expected = t(
        th("gb",    "sum", "cnt", "sum0", "min", "max"),
        tr("group1",     0.0, 2L, 0.0, -5D, 5D),
        tr("group2",     -3D, 2L, -3D, -13D, 10D),
        tr("group3",     Fixtures.NULL_DOUBLE, 0L, 0D, Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, DATA, expected);
  }

  @Test
  public void doubleWork1() throws Exception {
    HashAggregate conf = new HashAggregate(null,
      Arrays.asList(n("gb")),
      Arrays.asList(
        n("sum(d1)", "sum-d1"),
        n("count(d1)", "cnt-d1"),
        n("$sum0(d1)", "sum0-d1"),
        n("min(d1)", "min-d1"),
        n("max(d1)", "max-d1"),
        n("sum(d2)", "sum-d2"),
        n("count(d2)", "cnt-d2"),
        n("$sum0(d2)", "sum0-d2"),
        n("min(d2)", "min-d2"),
        n("max(d2)", "max-d2")
      ),
      true,
      1f);

    final Table expected = t(
      th("gb",    "sum-d1", "cnt-d1", "sum0-d1", "min-d1", "max-d1", "sum-d2", "cnt-d2", "sum0-d2", "min-d2", "max-d2"),
      tr("group1", 0.0, 2L, 0.0, 0.0,  0.0, -1.0, 2L, -1.0, -1.0, 0.0),
      tr("group2", -966.25, 2L, -966.25, -966.25, 0.0, -1873.67, 2L, -1873.67, -1023.42, -850.25),
      tr("group3", Fixtures.NULL_DOUBLE, 0L, 0.0D, Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE,
        Fixtures.NULL_DOUBLE, 0L, 0.0D, Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE));

    validateSingle(conf, VectorizedHashAggOperator.class, DATA_DOUBLE, expected);
  }

  private static final Table DATA_DOUBLE = t(
    th("gb",    "d1",    "d2"),
    tr("group1", 0.0,     -1.0),
    tr("group2", -966.25, -850.25),
    tr("group1", 0.0,     0.0),
    tr("group2", 0.0,     -1023.42),
    tr("group1", Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE),
    tr("group2", Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE),
    tr("group3", Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE),
    tr("group3", Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE)
  );

  @Test
  public void count1() throws Exception {

    HashAggregate conf = new HashAggregate(null,
        Arrays.asList(n("gb")),
        Arrays.asList(
            n("count(1)", "cnt")
            ),
        true,
        1f);

    final Table expected = t(
        th("gb",    "cnt"),
        tr("group1",     3L),
        tr("group2",     3L),
        tr("group3",     2L)
        );

    validateSingle(conf, VectorizedHashAggOperator.class, DATA, expected);
  }

  private static final Table DATA = t(
      th("gb", "myint", "mybigint", "myfloat", "mydouble"),
      tr("group1", 5, 5L, 5f, 5d),
      tr("group2", 10, 10L, 10f, 10d),
      tr("group1", -10, -10L, -10f, -5d),
      tr("group2", -13, -13L, -13f, -13d),
      tr("group1", Fixtures.NULL_INT, Fixtures.NULL_BIGINT, Fixtures.NULL_FLOAT, Fixtures.NULL_DOUBLE),
      tr("group2", Fixtures.NULL_INT, Fixtures.NULL_BIGINT, Fixtures.NULL_FLOAT, Fixtures.NULL_DOUBLE),
      tr("group3", Fixtures.NULL_INT, Fixtures.NULL_BIGINT, Fixtures.NULL_FLOAT, Fixtures.NULL_DOUBLE),
      tr("group3", Fixtures.NULL_INT, Fixtures.NULL_BIGINT, Fixtures.NULL_FLOAT, Fixtures.NULL_DOUBLE)
      );

  @Test
  public void largeSumWithResize() throws Exception {

    final List<NamedExpression> dim = Arrays.asList(n("c_mktsegment"));
    final List<NamedExpression> measure = Arrays.asList(
        n("sum(c_acctbal)", "sum"),
        n("count(1)", "cnt")
        );

    final Table expected = t(
        th("c_mktsegment", "sum", "cnt"),
        tr("BUILDING", 13588862194l, 30142l),
        tr("AUTOMOBILE", 13386684709l, 29752l),
        tr("MACHINERY", 13443886167l, 29949l),
        tr("HOUSEHOLD", 13587334117l, 30189l),
        tr("FURNITURE", 13425917787l, 29968l)
        );

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    try(AutoCloseable options = with(ExecConstants.MIN_HASH_TABLE_SIZE, 1)){
      validateSingle(conf, VectorizedHashAggOperator.class, TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, allocator), expected, 1000);
    }

    final HashAggregate conf2 = new HashAggregate(null, dim, measure, false, 1f);
    try(AutoCloseable options = with(ExecConstants.MIN_HASH_TABLE_SIZE, 1)){
      validateSingle(conf2, HashAggOperator.class, TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, allocator), expected, 1000);
    }
  }

}
