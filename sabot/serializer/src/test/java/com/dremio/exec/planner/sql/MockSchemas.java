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
package com.dremio.exec.planner.sql;

import static org.apache.calcite.sql.type.SqlTypeName.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.sql.type.SqlTypeName;

/** Hardcoded schemas used for query plan testing. */
public final class MockSchemas {
  public static final ImmutableList<ColumnSchema> CUSTOMER =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("C_CUSTKEY", INTEGER))
          .add(ColumnSchema.create("C_NAME", VARCHAR))
          .add(ColumnSchema.create("C_ADDRESS", VARCHAR))
          .add(ColumnSchema.create("C_NATIONKEY", INTEGER))
          .add(ColumnSchema.create("C_PHONE", VARCHAR))
          .add(ColumnSchema.create("C_ACCTBAL", DOUBLE))
          .add(ColumnSchema.create("C_MKTSEGMENT", VARCHAR))
          .add(ColumnSchema.create("C_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> DEPT =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("DEPTNO", INTEGER))
          .add(ColumnSchema.create("NAME", VARCHAR, 10))
          .build();

  public static final ImmutableList<ColumnSchema> EMP =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("EMPNO", INTEGER))
          .add(ColumnSchema.create("ENAME", VARCHAR, 20))
          .add(ColumnSchema.create("JOB", VARCHAR, 10))
          .add(ColumnSchema.create("MGR", INTEGER))
          .add(ColumnSchema.create("HIREDATE", TIMESTAMP))
          .add(ColumnSchema.create("SAL", INTEGER))
          .add(ColumnSchema.create("COMM", INTEGER))
          .add(ColumnSchema.create("DEPTNO", INTEGER))
          .add(ColumnSchema.create("SLACKER", BOOLEAN))
          .build();

  public static final ImmutableList<ColumnSchema> LINEITEM =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("L_ORDERKEY", INTEGER))
          .add(ColumnSchema.create("L_PARTKEY", INTEGER))
          .add(ColumnSchema.create("L_SUPPKEY", INTEGER))
          .add(ColumnSchema.create("L_LINENUMBER", INTEGER))
          .add(ColumnSchema.create("L_QUANTITY", INTEGER))
          .add(ColumnSchema.create("L_EXTENDEDPRICE", DOUBLE))
          .add(ColumnSchema.create("L_DISCOUNT", DOUBLE))
          .add(ColumnSchema.create("L_TAX", DOUBLE))
          .add(ColumnSchema.create("L_RETURNFLAG", VARCHAR))
          .add(ColumnSchema.create("L_LINESTATUS", VARCHAR))
          .add(ColumnSchema.create("L_SHIPDATE", DATE))
          .add(ColumnSchema.create("L_COMMITDATE", DATE))
          .add(ColumnSchema.create("L_RECEIPTDATE", DATE))
          .add(ColumnSchema.create("L_SHIPINSTRUCT", VARCHAR))
          .add(ColumnSchema.create("L_SHIPMODE", VARCHAR))
          .add(ColumnSchema.create("L_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> NATION =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("N_NATIONKEY", INTEGER))
          .add(ColumnSchema.create("N_NAME", VARCHAR))
          .add(ColumnSchema.create("N_REGIONKEY", INTEGER))
          .add(ColumnSchema.create("N_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> ORDER =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("O_ORDERKEY", INTEGER))
          .add(ColumnSchema.create("O_CUSTKEY", INTEGER))
          .add(ColumnSchema.create("O_ORDERSTATUS", VARCHAR))
          .add(ColumnSchema.create("O_TOTALPRICE", DOUBLE))
          .add(ColumnSchema.create("O_ORDERDATE", DATE))
          .add(ColumnSchema.create("O_ORDERPRIORITY", VARCHAR))
          .add(ColumnSchema.create("O_CLERK", VARCHAR))
          .add(ColumnSchema.create("O_SHIPPRIORITY", VARCHAR))
          .add(ColumnSchema.create("O_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> PART =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("P_PARTKEY", INTEGER))
          .add(ColumnSchema.create("P_NAME", VARCHAR))
          .add(ColumnSchema.create("P_MFGR", VARCHAR))
          .add(ColumnSchema.create("P_BRAND", VARCHAR))
          .add(ColumnSchema.create("P_TYPE", VARCHAR))
          .add(ColumnSchema.create("P_SIZE", INTEGER))
          .add(ColumnSchema.create("P_CONTAINER", VARCHAR))
          .add(ColumnSchema.create("P_RETAILPRICE", DOUBLE))
          .add(ColumnSchema.create("P_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> PART_SUPPLIER =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("PS_PARTKEY", INTEGER))
          .add(ColumnSchema.create("PS_SUPPKEY", INTEGER))
          .add(ColumnSchema.create("PS_AVAILQTY", INTEGER))
          .add(ColumnSchema.create("PS_SUPPLYCOST", DOUBLE))
          .add(ColumnSchema.create("PS_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> REGION =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("R_REGIONKEY", INTEGER))
          .add(ColumnSchema.create("R_NAME", VARCHAR))
          .add(ColumnSchema.create("R_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> SALGRADE =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("GRADE", INTEGER))
          .add(ColumnSchema.create("LOSAL", INTEGER))
          .add(ColumnSchema.create("HIGHSAL", INTEGER))
          .build();

  public static final ImmutableList<ColumnSchema> SUPPLIER =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("S_SUPPKEY", INTEGER))
          .add(ColumnSchema.create("S_NAME", VARCHAR))
          .add(ColumnSchema.create("S_ADDRESS", VARCHAR))
          .add(ColumnSchema.create("S_NATIONKEY", INTEGER))
          .add(ColumnSchema.create("S_PHONE", VARCHAR))
          .add(ColumnSchema.create("S_ACCTBAL", DOUBLE))
          .add(ColumnSchema.create("S_COMMENT", VARCHAR))
          .build();

  public static final ImmutableList<ColumnSchema> TEST_SCHEMA =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("ID", INTEGER))
          .add(ColumnSchema.create("A", INTEGER))
          .add(ColumnSchema.create("B", INTEGER))
          .add(ColumnSchema.create("C", INTEGER))
          .add(ColumnSchema.create("D", DATE))
          .build();

  public static final ImmutableList<ColumnSchema> MIXED_TYPE_SCHEMA =
      ImmutableList.<ColumnSchema>builder()
          .add(ColumnSchema.create("INTEGER_COLUMN", INTEGER))
          .add(ColumnSchema.create("BOOLEAN_COLUMN", BOOLEAN))
          .add(ColumnSchema.create("DECIMAL_COLUMN", DECIMAL))
          .add(ColumnSchema.create("FLOAT_COLUMN", FLOAT))
          .add(ColumnSchema.create("DOUBLE_COLUMN", DOUBLE))
          .add(ColumnSchema.create("DATE_COLUMN", DATE))
          .add(ColumnSchema.create("TIME_COLUMN", TIME))
          .add(ColumnSchema.create("TIMESTAMP_COLUMN", TIMESTAMP))
          .add(ColumnSchema.create("VARCHAR_COLUMN", VARCHAR))
          .add(ColumnSchema.create("BINARY_COLUMN", BINARY))
          .add(ColumnSchema.create("ANY_COLUMN", ANY))
          .build();

  private MockSchemas() {}

  /** Schema of a single column in a DremioTable. */
  public static final class ColumnSchema {
    private final String name;
    private final SqlTypeName sqlTypeName;
    private final Optional<Integer> precision;

    private ColumnSchema(String name, SqlTypeName sqlTypeName, Optional<Integer> precision) {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(sqlTypeName);
      Preconditions.checkNotNull(precision);

      this.name = name;
      this.sqlTypeName = sqlTypeName;
      this.precision = precision;
    }

    public String getName() {
      return this.name;
    }

    public SqlTypeName getSqlTypeName() {
      return this.sqlTypeName;
    }

    public Optional<Integer> getPrecision() {
      return this.precision;
    }

    public static ColumnSchema create(String name, SqlTypeName sqlTypeName) {
      return ColumnSchema.create(name, sqlTypeName, Optional.empty());
    }

    public static ColumnSchema create(String name, SqlTypeName sqlTypeName, int precision) {
      return ColumnSchema.create(name, sqlTypeName, Optional.of(precision));
    }

    private static ColumnSchema create(
        String name, SqlTypeName sqlTypeName, Optional<Integer> precision) {
      return new ColumnSchema(name.toUpperCase(Locale.ROOT), sqlTypeName, precision);
    }
  }
}
