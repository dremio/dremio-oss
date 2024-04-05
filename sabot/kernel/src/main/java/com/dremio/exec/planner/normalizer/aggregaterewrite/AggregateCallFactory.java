/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import com.dremio.exec.expr.fn.hll.HyperLogLog;
import com.dremio.exec.expr.fn.tdigest.TDigest;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Factory to create AggregateCalls */
public final class AggregateCallFactory {
  private AggregateCallFactory() {}

  public static AggregateCall tDigest(
      RelDataType type, boolean distinct, int argIndex, String name) {
    return AggregateCall.create(
        new TDigest.SqlTDigestAggFunction(type),
        distinct,
        // TDigest is by definition approximate, so we don't need to pass it in.
        false,
        Collections.singletonList(argIndex),
        -1,
        RelCollations.EMPTY,
        type,
        name);
  }

  public static AggregateCall percentileCont(
      boolean distinct, boolean approximate, int argIndex, RelCollation collation, String name) {
    return AggregateCall.create(
        SqlStdOperatorTable.PERCENTILE_CONT,
        distinct,
        approximate,
        ImmutableList.of(argIndex),
        /*filterArg*/ -1,
        collation,
        JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.DOUBLE),
        name);
  }

  public static AggregateCall ndv(int arg, int filterArg, String name) {
    return AggregateCall.create(
        DremioSqlOperatorTable.NDV,
        // NDV is by definition DISTINCT, so we don't need to pass it in
        false,
        // NDV is by definition APPROXIMATE, so we don't need to pass it in
        false,
        ImmutableList.of(arg),
        filterArg,
        // There is no ordering for NDV
        RelCollations.EMPTY,
        JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.BIGINT),
        name);
  }

  public static AggregateCall hll(int arg, int filterArg, String name) {
    return AggregateCall.create(
        DremioSqlOperatorTable.HLL,
        // HLL is by definition DISTINCT
        false,
        ImmutableList.of(arg),
        filterArg,
        JavaTypeFactoryImpl.INSTANCE.createSqlType(
            SqlTypeName.VARBINARY, HyperLogLog.HLL_VARBINARY_SIZE),
        name);
  }
}
