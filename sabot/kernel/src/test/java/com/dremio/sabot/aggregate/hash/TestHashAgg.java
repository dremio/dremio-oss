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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import com.dremio.sabot.op.aggregate.vectorized.nospill.VectorizedHashAggOperatorNoSpill;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.Generator;
import com.dremio.sabot.op.aggregate.hash.HashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;

import io.airlift.tpch.GenerationDefinition.TpchTable;
import io.airlift.tpch.TpchGenerator;
import io.netty.buffer.ArrowBuf;

public class TestHashAgg extends BaseTestOperator {

  private void validateAgg(HashAggregate conf, TpchTable table, double scale, Fixtures.Table expectedResult) throws Exception {
    /* test with row-wise hashagg operator */
    HashAggregate vanillaConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), false, conf.getCardinality());
    validateSingle(vanillaConf, HashAggOperator.class, table, scale, expectedResult);

    /* test with vectorized hashagg operator that supports spilling */
    try (AutoCloseable options1 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      for (int i = 0; i <= 5; i++) {
        try (AutoCloseable options2 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS, 1 << i)) {
          HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
          validateSingle(vectorizedConf, VectorizedHashAggOperator.class, table, scale, expectedResult);
        }
      }
    }

    /* test with old vectorized hashagg operator -- that does not support spilling */
    try (AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, false)) {
      HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
      validateSingle(vectorizedConf, VectorizedHashAggOperatorNoSpill.class, table, scale, expectedResult);
    }
  }

  private void validateAgg(HashAggregate conf, Fixtures.Table input, Fixtures.Table expectedResult) throws Exception {
    /* test with row-wise hashagg operator */
    HashAggregate vanillaConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), false, conf.getCardinality());
    validateSingle(vanillaConf, HashAggOperator.class, input, expectedResult);

    /* test with vectorized hashagg operator that supports spilling */
    try (AutoCloseable options1 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      for (int i = 0; i <= 5; i++) {
        try (AutoCloseable options2 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS, 1 << i)) {
          HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
          validateSingle(vectorizedConf, VectorizedHashAggOperator.class, input, expectedResult);
        }
      }
    }

    /* test with old vectorized hashagg operator -- that does not support spilling */
    try (AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, false)) {
      HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
      validateSingle(vectorizedConf, VectorizedHashAggOperatorNoSpill.class, input, expectedResult);
    }
  }

  private void validateAggGenerated(HashAggregate conf, Fixtures.Table input, Fixtures.Table expectedResult) throws Exception {
    /* test with row-wise hashagg operator */
    HashAggregate vanillaConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), false, conf.getCardinality());
    validateSingle(vanillaConf, HashAggOperator.class, input.toGenerator(allocator), expectedResult, 1000);

    /* test with vectorized hashagg operator that supports spilling */
    try (AutoCloseable options1 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      for (int i = 0; i <= 5; i++) {
        try (AutoCloseable options2 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS, 1 << i)) {
          HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
          validateSingle(vectorizedConf, VectorizedHashAggOperator.class, input.toGenerator(allocator), expectedResult, 1000);
        }
      }
    }

    /* test with old vectorized hashagg operator -- that does not support spilling */
    try (AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, false)) {
      HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
      validateSingle(vectorizedConf, VectorizedHashAggOperatorNoSpill.class, input.toGenerator(allocator), expectedResult, 1000);
    }
  }

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
      tr("MIDDLE EAST", 4L, 1L))
      .orderInsensitive();

    validateAgg(conf, TpchTable.REGION, 0.1, expected);
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

    Fixtures.DataRow row1 = tr("AFRICA", 0L, 1L);
    Fixtures.DataRow row2 = tr("AMERICA", 1L, 1L);
    Fixtures.DataRow row3 = tr("ASIA", 2L, 1L);
    Fixtures.DataRow row4 = tr("EUROPE", 3L, 1L);
    Fixtures.DataRow row5 = tr("MIDDLE EAST", 4L, 1L);

    final Table expected = t(
      th("r_name",    "sum", "cnt"),
      tr("AFRICA", 0L, 1L),
      tr("AMERICA", 1L, 1L),
      tr("ASIA", 2L, 1L),
      tr("EUROPE", 3L, 1L),
      tr("MIDDLE EAST", 4L, 1L))
      .orderInsensitive();

    validateAgg(conf, TpchTable.REGION, 0.1, expected);
  }

  @Test
  public void booleanWork() throws Exception {
    HashAggregate conf = new HashAggregate(null,
                                           Arrays.asList(n("boolean_col")),
                                           Arrays.asList(
                                             n("sum(int_col)", "sum_int_col"),
                                             n("sum(bigint_col)", "sum_bigint_col")
                                           ),
                                           true,
                                           1f);

    final Table expected = t(
      th("boolean_col", "sum_int_col", "sum_bigint_col"),
      tr(true, 77L, 937L),
      tr(false, -15L, 99915L),
      tr(Fixtures.NULL_BOOLEAN, 20L, 300L))
      .orderInsensitive();

    validateAgg(conf, DATA_BOOLEAN, expected);
  }

  private static final Table DATA_BOOLEAN = t(
    th("boolean_col", "int_col", "bigint_col"),
    tr(true, 0,    -1L),
    tr(false, -96, -85L),
    tr(true, 0,   0L),
    tr(true, 0,   -102L),
    tr(true, 77, 1040L),
    tr(false, 81, 100000L),
    tr(true, Fixtures.NULL_INT, Fixtures.NULL_BIGINT),
    tr(false, Fixtures.NULL_INT, Fixtures.NULL_BIGINT),
    tr(false, Fixtures.NULL_INT, Fixtures.NULL_BIGINT),
    tr(Fixtures.NULL_BOOLEAN, Fixtures.NULL_INT, Fixtures.NULL_BIGINT),
    tr(Fixtures.NULL_BOOLEAN, 20, 300L)
  );

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
      tr("group1", -5L, 2L, -5L, -10, 5),
      tr("group2", -3L, 2L, -3L, -13, 10),
      tr("group3", Fixtures.NULL_BIGINT, 0L, 0l, Fixtures.NULL_INT, Fixtures.NULL_INT))
      .orderInsensitive();

    validateAgg(conf, DATA, expected);
  }

  @Test
  public void intWork1() throws Exception {
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
      tr("group1", 0L, 2L, 0L, 0,  0, -1L, 2L, -1L, -1, 0),
      tr("group2", -96L, 2L, -96L, -96, 0, -187L, 2L, -187L, -102, -85),
      tr("group3", Fixtures.NULL_BIGINT, 0L, 0L, Fixtures.NULL_INT, Fixtures.NULL_INT,
         Fixtures.NULL_BIGINT, 0L, 0L, Fixtures.NULL_INT, Fixtures.NULL_INT))
      .orderInsensitive();

    validateAgg(conf, DATA_INT, expected);
  }

  private static final Table DATA_INT = t(
    th("gb",    "d1", "d2"),
    tr("group1", 0,    -1),
    tr("group2", -96, -85),
    tr("group1", 0,   0),
    tr("group2", 0,   -102),
    tr("group1", Fixtures.NULL_INT, Fixtures.NULL_INT),
    tr("group2", Fixtures.NULL_INT, Fixtures.NULL_INT),
    tr("group3", Fixtures.NULL_INT, Fixtures.NULL_INT),
    tr("group3", Fixtures.NULL_INT, Fixtures.NULL_INT)
  );

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

    Fixtures.DataRow row1 = tr("group1", -5L, 2L, -5l, -10l, 5l);
    Fixtures.DataRow row2 = tr("group2", -3L, 2L, -3l, -13l, 10l);
    Fixtures.DataRow row3 = tr("group3", Fixtures.NULL_BIGINT, 0L, 0l, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT);

    final Table expected = t(
      th("gb",    "sum", "cnt", "sum0", "min", "max"),
      tr("group1", -5L, 2L, -5L, -10L, 5L),
      tr("group2", -3L, 2L, -3L, -13L, 10L),
      tr("group3", Fixtures.NULL_BIGINT, 0L, 0l, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT))
      .orderInsensitive();

    validateAgg(conf, DATA, expected);
  }

  @Test
  public void bigintWork1() throws Exception {
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
      tr("group1", 0L, 2L, 0L, 0L,  0L, -125000L, 2L, -125000L, -125000L, 0L),
      tr("group2", -126000000L, 2L, -126000000L, -126000000L, 0L, -10262800000L, 2L, -10262800000L, -10254300000L, -8500000L),
      tr("group3", Fixtures.NULL_BIGINT, 0L, 0L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT,
         Fixtures.NULL_BIGINT, 0L, 0L, Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT))
      .orderInsensitive();

    validateAgg(conf, DATA_BIGINT, expected);
  }

  private static final Table DATA_BIGINT = t(
    th("gb",    "d1", "d2"),
    tr("group1", 0L,    -125000L),
    tr("group2", -126000000L, -8500000L),
    tr("group1", 0L,   0L),
    tr("group2", 0L,   -10254300000L),
    tr("group1", Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
    tr("group2", Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
    tr("group3", Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT),
    tr("group3", Fixtures.NULL_BIGINT, Fixtures.NULL_BIGINT)
  );

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
      tr("group1", -5D, 2L, -5D, -10f, 5f),
      tr("group2", -3D, 2L, -3D, -13f, 10f),
      tr("group3", Fixtures.NULL_DOUBLE, 0L, 0D, Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT))
      .orderInsensitive();

    validateAgg(conf, DATA, expected);
  }

  @Test
  public void floatWork1() throws Exception {
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
      tr("group1", 0.0d, 2L, 0.0d, 0.0f,  0.0f, -1.0D, 2L, -1.0D, -1.0f, 0.0f),
      tr("group2", -96.25D, 2L, -96.25D, -96.25f, 0.0f, -187.67D, 2L, -187.67D, -102.42f, -85.25f),
      tr("group3", Fixtures.NULL_DOUBLE, 0L, 0.0D, Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT,
         Fixtures.NULL_DOUBLE, 0L, 0.0D, Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT))
      .orderInsensitive();

    validateAgg(conf, DATA_FLOAT, expected);
  }

  private static final Table DATA_FLOAT = t(
    th("gb",    "d1",    "d2"),
    tr("group1", 0.0f,     -1.0f),
    tr("group2", -96.25f, -85.25f),
    tr("group1", 0.0f,     0.0f),
    tr("group2", 0.0f,     -102.42f),
    tr("group1", Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT),
    tr("group2", Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT),
    tr("group3", Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT),
    tr("group3", Fixtures.NULL_FLOAT, Fixtures.NULL_FLOAT)
  );


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
      tr("group1", 0D, 2L, 0D, -5D, 5D),
      tr("group2", -3D, 2L, -3D, -13D, 10D),
      tr("group3", Fixtures.NULL_DOUBLE, 0L, 0D, Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE))
      .orderInsensitive();

    validateAgg(conf, DATA, expected);
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
         Fixtures.NULL_DOUBLE, 0L, 0.0D, Fixtures.NULL_DOUBLE, Fixtures.NULL_DOUBLE))
      .orderInsensitive();

    validateAgg(conf, DATA_DOUBLE, expected);
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

    Fixtures.DataRow row1 = tr("group1", 3L);
    Fixtures.DataRow row2 = tr("group2", 3L);
    Fixtures.DataRow row3 = tr("group3", 2L);

    final Table expected = t(
      th("gb",    "cnt"),
      tr("group1", 3L),
      tr("group2", 3L),
      tr("group3", 2L))
      .orderInsensitive();

    validateAgg(conf, DATA, expected);
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
      tr("BUILDING", 13588862194L, 30142L),
      tr("AUTOMOBILE", 13386684709L, 29752L),
      tr("MACHINERY", 13443886167L, 29949L),
      tr("HOUSEHOLD", 13587334117L, 30189L),
      tr("FURNITURE", 13425917787L, 29968L))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    try(AutoCloseable options1 = with(ExecConstants.MIN_HASH_TABLE_SIZE, 1);
        AutoCloseable options2 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      /* test with vectorized hashagg that supports spilling */
      for (int i = 0; i <= 5; i++) {
        try (AutoCloseable options3 = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_NUMPARTITIONS, 1 << i)) {
          validateSingle(conf, VectorizedHashAggOperator.class, TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, allocator), expected, 1000);
        }
      }

      /* test with row-wise hashagg */
      final HashAggregate conf2 = new HashAggregate(null, dim, measure, false, 1f);
      validateSingle(conf2, HashAggOperator.class, TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, allocator), expected, 1000);

      /* test with vectorized hashagg that does not support spilling */
      try(AutoCloseable options = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, false)) {
        validateSingle(conf, VectorizedHashAggOperatorNoSpill.class, TpchGenerator.singleGenerator(TpchTable.CUSTOMER, 1, allocator), expected, 1000);
      }
    }
  }

  @Test
  public void decimalWork() throws Exception {
    final Table inputData = t(
      th("dec_x", "dec_y", "str_z"),
      tr(new BigDecimal(10.0), new BigDecimal(2.0), "a"),
      tr(new BigDecimal(20.0), new BigDecimal(3.0), "b"),
      tr(new BigDecimal(30.0), new BigDecimal(4.0), "c"),
      tr(new BigDecimal(20.0), new BigDecimal(5.0), "d"),
      tr(new BigDecimal(20.0), new BigDecimal(6.0), "e"),
      tr(new BigDecimal(10.0), new BigDecimal(7.0), "f")
    );

    final List<NamedExpression> dim = Arrays.asList(n("dec_x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("sum(dec_y)", "sum_y"),
      n("count(1)", "cnt"),
      n("$sum0(dec_y)", "sum0"),
      n("min(dec_y)", "min"),
      n("max(dec_y)", "max")
    );

    final Table expected = t(
      th("dec_x", "sum_y", "cnt", "sum0", "min", "max"),
      tr(new BigDecimal(10.0), 9.0d, 2L, 9.0d, 2.0d, 7.0d),
      tr(new BigDecimal(20.0), 14.0d, 3L, 14.0d, 3.0d, 6.0d),
      tr(new BigDecimal(30.0), 4.0d, 1L, 4.0d, 4.0d,4.0d))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  @Test
  public void decimalSumVsSum0() throws Exception {
    final Table inputData = t(
      th("dec_x", "dec_y", "str_z"),
      tr(new BigDecimal(10.0), Fixtures.NULL_DECIMAL, "a"),
      tr(new BigDecimal(20.0), new BigDecimal(2.0), "b"),
      tr(new BigDecimal(30.0), Fixtures.NULL_DECIMAL, "c"),
      tr(new BigDecimal(20.0), Fixtures.NULL_DECIMAL, "d"),
      tr(new BigDecimal(20.0), new BigDecimal(5.0), "e"),
      tr(new BigDecimal(10.0), Fixtures.NULL_DECIMAL, "f")
    );

    final List<NamedExpression> dim = Arrays.asList(n("dec_x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("sum(dec_y)", "sum_y"),
      n("$sum0(dec_y)", "sum0")
    );

    final Table expected = t(
      th("dec_x", "sum_y", "sum0"),
      tr(new BigDecimal(10.0), Fixtures.NULL_DOUBLE, 0.0d),
      tr(new BigDecimal(20.0), 7.0d, 7.0d),
      tr(new BigDecimal(30.0), Fixtures.NULL_DOUBLE, 0.0d))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  @Test
  public void bitWork() throws Exception {
    final int numSixes = 100;
    final Fixtures.DataRow lotsOfSixes[] = new Fixtures.DataRow[numSixes];
    for (int i = 0; i < numSixes; i++) {
      lotsOfSixes[i] = tr(6, true);
    }
    final Table inputData = t(
      th("x", "y"),
      new Fixtures.DataBatch(lotsOfSixes),
      new Fixtures.DataBatch(
        tr(6, true),
        tr(5, true),
        tr(7, false),
        tr(13, true),
        tr(7, true),
        tr(11, Fixtures.NULL_BOOLEAN),
        tr(11, true),
        tr(6, true),
        tr(9, true),
        tr(10, Fixtures.NULL_BOOLEAN),
        tr(8, false),
        tr(13, Fixtures.NULL_BOOLEAN),
        tr(12, false),
        tr(10, false),
        tr(6, true),
        tr(12, Fixtures.NULL_BOOLEAN),
        tr(4, false),
        tr(9, false),
        tr(8, false)
      )
    );

    final List<NamedExpression> dim = Arrays.asList(n("x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("count(y)", "cnt"),
      n("min(y)", "min"),
      n("max(y)", "max")
    );

    final Table expected = t(
      th("x", "cnt", "min", "max"),
      tr(4, 1l, false, false),
      tr(5, 1l, true, true),
      tr(6, numSixes + 3l, true, true),
      tr(7, 2l, false, true),
      tr(8, 2l, false, false),
      tr(9, 2l, false, true),
      tr(10, 1l, false, false),
      tr(11, 1l, true, true),
      tr(12, 1l, false, false),
      tr(13, 1l, true, true))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  /*
   * BitGenerator for all four combinations :
   * index 0 : valid and true
   * index 1 : valid and false
   * index 2 : invalid and true
   * index 3 : invalid and true
   */
  private static final class BitGenerator implements Generator {
    private final VectorContainer result;
    private final BitVector bitVector;
    private int idx = 0;
    private int rows = 4;

    public BitGenerator(BufferAllocator allocator, String column) {
      result = new VectorContainer(allocator);
      bitVector = result.addOrGet(column, Types.optional(MinorType.BIT), BitVector.class);
      result.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }

    @Override
    public int next(int records) {
      int count = Math.min(rows - idx, records);
      if (count == 0) {
        return 0;
      }

      int[] validities = new int[] {1, 1, 0, 0};
      int[] values = new int[] {1, 0, 1, 0};

      result.allocateNew();
      ArrowBuf validityBuf = bitVector.getValidityBuffer();
      ArrowBuf valueBuf = bitVector.getDataBuffer();
      for (int i = 0; i < count; ++i) {
        BitVectorHelper.setValidityBit(validityBuf, i, validities[idx]);
        BitVectorHelper.setValidityBit(valueBuf, i, values[idx]);
        ++idx;
      }

      result.setAllCount(count);
      return count;
    }

    @Override
    public VectorAccessible getOutput() {
      return result;
    }

    @Override
    public void close() {
      result.close();
    }
  }

  @Test
  public void bitWorkWithNulls() throws Exception {
    String column = "x";
    final List<NamedExpression> dim = Arrays.asList(n(column));
    final List<NamedExpression> measure = Arrays.asList();

    final Table expected = t(
      th(column),
      tr(true),
      tr(false),
      tr(Fixtures.NULL_BOOLEAN))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    try (BitGenerator generator = new BitGenerator(allocator, column)) {
      HashAggregate vanillaConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), false, conf.getCardinality());
      validateSingle(vanillaConf, HashAggOperator.class, generator, expected, 1000);
    }

    try (AutoCloseable useSpillingAgg = with(VectorizedHashAggOperator.VECTORIZED_HASHAGG_USE_SPILLING_OPERATOR, true)) {
      try (BitGenerator generator = new BitGenerator(allocator, column)) {
        HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
        validateSingle(vectorizedConf, VectorizedHashAggOperator.class, generator, expected, 1000);
      }
    }

    try (BitGenerator generator = new BitGenerator(allocator, column)) {
      HashAggregate vectorizedConf = new HashAggregate(conf.getChild(), conf.getGroupByExprs(), conf.getAggrExprs(), true, conf.getCardinality());
      validateSingle(vectorizedConf, VectorizedHashAggOperatorNoSpill.class, generator, expected, 1000);
    }
  }

  @Ignore("ARROW-1984")
  @Test
  public void dateWork() throws Exception {
    final Table inputData = t(
      th("x", "y"),
      tr(4, Fixtures.date("2017-1-1")),
      tr(5, Fixtures.date("2018-6-17")),
      tr(7, Fixtures.date("2017-12-1")),
      tr(5, Fixtures.date("2011-1-17")),
      tr(7, Fixtures.date("2017-11-30")),
      tr(6, Fixtures.date("2020-2-29"))
    );

    final List<NamedExpression> dim = Arrays.asList(n("x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("count(y)", "cnt"),
      n("min(y)", "min"),
      n("max(y)", "max")
    );

    final Table expected = t(
      th("x", "cnt", "min", "max"),
      tr(4, 1l, Fixtures.date("2017-1-1"), Fixtures.date("2017-1-1")),
      tr(5, 2l, Fixtures.date("2011-1-17"), Fixtures.date("2018-6-17")),
      tr(6, 1l, Fixtures.date("2020-2-29"), Fixtures.date("2020-2-29")),
      tr(7, 2l, Fixtures.date("2017-11-30"), Fixtures.date("2017-12-1")))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  @Test
  public void intervalWork() throws Exception {
    final Table inputData = t(
      th("x", "days", "months"),
      tr(5, Fixtures.interval_day(9, 20), Fixtures.interval_year(1, 3)),
      tr(5, Fixtures.interval_day(1, 100), Fixtures.interval_year(2, 0)),
      tr(4, Fixtures.interval_day(2, 0), Fixtures.interval_year(0, 2)),
      tr(6, Fixtures.interval_day(0, 30), Fixtures.interval_year(0, 7)),
      tr(7, Fixtures.interval_day(1, 0), Fixtures.interval_year(0, 7)),
      tr(6, Fixtures.interval_day(0, 30), Fixtures.interval_year(0, 7))
    );

    final List<NamedExpression> dim = Arrays.asList(n("x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("count(x)", "cnt"),
      n("min(days)", "min_days"),
      n("max(days)", "max_days"),
      n("min(months)", "min_months"),
      n("max(months)", "max_months")
    );

    final Table expected = t(
      th("x", "cnt", "min_days", "max_days", "min_months", "max_months"),
      tr(4, 1l, Fixtures.interval_day(2, 0), Fixtures.interval_day(2, 0),
         Fixtures.interval_year(0, 2), Fixtures.interval_year(0, 2)),
      tr(5, 2l, Fixtures.interval_day(1, 100), Fixtures.interval_day(9, 20),
         Fixtures.interval_year(1, 3), Fixtures.interval_year(2, 0)),
      tr(6, 2l, Fixtures.interval_day(0, 30), Fixtures.interval_day(0, 30),
         Fixtures.interval_year(0, 7), Fixtures.interval_year(0, 7)),
      tr(7, 1l, Fixtures.interval_day(1, 0), Fixtures.interval_day(1, 0),
         Fixtures.interval_year(0, 7), Fixtures.interval_year(0, 7)))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  @Ignore("ARROW-1984")
  @Test
  public void timeWork() throws Exception {
    final Table inputData = t(
      th("x", "y"),
      tr(4, Fixtures.time("01:00")),
      tr(5, Fixtures.time("16:00")),
      tr(7, Fixtures.time("08:00")),
      tr(5, Fixtures.time("17:00")),
      tr(7, Fixtures.time("09:00")),
      tr(6, Fixtures.time("19:00"))
    );

    final List<NamedExpression> dim = Arrays.asList(n("x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("count(y)", "cnt"),
      n("min(y)", "min"),
      n("max(y)", "max")
    );

    final Table expected = t(
      th("x", "cnt", "min", "max"),
      tr(4, 1l, Fixtures.time("01:00"), Fixtures.time("01:00")),
      tr(5, 2l, Fixtures.time("16:00"), Fixtures.time("17:00")),
      tr(6, 1l, Fixtures.time("19:00"), Fixtures.time("19:00")),
      tr(7, 2l, Fixtures.time("09:00"), Fixtures.time("09:00")))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, true, 1f);
    validateAggGenerated(conf, inputData, expected);
  }

  @Test
  public void timestampWork() throws Exception {
    final Table inputData = t(
      th("x", "y"),
      tr(4, Fixtures.ts("2017-1-1T11:00")),
      tr(5, Fixtures.ts("2018-6-17T10:00")),
      tr(7, Fixtures.ts("2017-12-1T07:00")),
      tr(5, Fixtures.ts("2011-1-17T19:00")),
      tr(7, Fixtures.ts("2017-11-30T21:00")),
      tr(6, Fixtures.ts("2020-2-29T12:00"))
    );

    final List<NamedExpression> dim = Arrays.asList(n("x"));
    final List<NamedExpression> measure = Arrays.asList(
      n("count(y)", "cnt"),
      n("min(y)", "min"),
      n("max(y)", "max")
    );

    final Table expected = t(
      th("x", "cnt", "min", "max"),
      tr(4, 1l, Fixtures.ts("2017-1-1T11:00"), Fixtures.ts("2017-1-1T11:00")),
      tr(5, 2l, Fixtures.ts("2011-1-17T19:00"), Fixtures.ts("2018-6-17T10:00")),
      tr(6, 1l, Fixtures.ts("2020-2-29T12:00"), Fixtures.ts("2020-2-29T12:00")),
      tr(7, 2l, Fixtures.ts("2017-11-30T21:00"), Fixtures.ts("2017-12-1T07:00")))
      .orderInsensitive();

    final HashAggregate conf = new HashAggregate(null, dim, measure, false, 1f);
    validateAggGenerated(conf, inputData, expected);
  }
}
