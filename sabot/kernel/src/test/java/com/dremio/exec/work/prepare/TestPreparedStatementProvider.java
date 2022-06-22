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
package com.dremio.exec.work.prepare;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.util.List;

import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.ColumnSearchability;
import com.dremio.exec.proto.UserProtos.ColumnUpdatability;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.PreparedStatement;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.ResultColumnMetadata;
import com.dremio.exec.store.ischema.InfoSchemaConstants;
import com.google.common.collect.ImmutableList;

/**
 * Tests for creating and executing prepared statements.
 */
public class TestPreparedStatementProvider extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPreparedStatementProvider.class);

  /**
   * Simple query.
   * @throws Exception
   */
  @Test
  public void simple() throws Exception {
    String query = "SELECT * FROM cp.\"region.json\" ORDER BY region_id LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("region_id", "BIGINT", true, 20, 0, 0, true, Long.class.getName()),
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_state_province", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_region", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_country", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district_id", "BIGINT", true, 20, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("region_id", "sales_city", "sales_state_province", "sales_district",
            "sales_region", "sales_country", "sales_district_id")
        .baselineValues(0L, "None", "None", "No District", "No Region", "No Country", 0L)
        .go();
  }

  /**
   * Create a prepared statement for a query that has GROUP BY clause in it
   */
  @Test
  public void groupByQuery() throws Exception {
    String query = "SELECT sales_city, count(*) as cnt FROM cp.\"region.json\" " +
        "GROUP BY sales_city ORDER BY sales_city DESC LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("cnt", "BIGINT", true, 20, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("sales_city", "cnt")
        .baselineValues("Yakima", 1L)
        .go();
  }

  /**
   * Create a prepared statement for a query that joins two tables and has ORDER BY clause.
   */
  @Test
  public void joinOrderByQuery() throws Exception {
    String query = "SELECT l.l_quantity, l.l_shipdate, o.o_custkey FROM cp.\"tpch/lineitem.parquet\" l JOIN cp.\"tpch/orders.parquet\" o " +
        "ON l.l_orderkey = o.o_orderkey LIMIT 2";

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("l_quantity", "DOUBLE", true, 24, 0, 0, true, Double.class.getName()),
        new ExpectedColumnResult("l_shipdate", "DATE", true, 10, 0, 0, false, Date.class.getName()),
        new ExpectedColumnResult("o_custkey", "INTEGER", true, 11, 0, 0, true, Integer.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a parser failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryParserError() throws Exception {
    createPrepareStmt("BLAH BLAH", true, ErrorType.PARSE);
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a validation failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryValidationError() throws Exception {
    createPrepareStmt("SELECT * sdflkgdh", true, ErrorType.VALIDATION);
  }

  @Test
  public void invalidPrepareHandle() throws Exception {
    final ServerPreparedStatementState state = ServerPreparedStatementState.newBuilder()
        .setHandle(-23) // Handles start from 0 in the sever. This is relying on server implementation detail. May need to find a better way to test it.
        .setSqlQuery("SELECT sales_city, count(*) as cnt FROM cp.\"region.json\" " +
            "GROUP BY sales_city ORDER BY sales_city DESC LIMIT 2")
        .setPrepareId(QueryId.newBuilder().build())
        .build();
    PreparedStatement preparedStatement =
        PreparedStatement.newBuilder()
            .setServerHandle(PreparedStatementHandle.newBuilder().setServerInfo(state.toByteString()).build())
            .build();

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("sales_city", "cnt")
        .baselineValues("Yakima", 1L)
        .baselineValues("Woodland Hills", 1L)
        .go();
  }

  /* Helper method which creates a prepared statement for given query. */
  private static PreparedStatement createPrepareStmt(String query, boolean expectFailure, ErrorType errorType) throws Exception {
    CreatePreparedStatementResp resp = client.createPreparedStatement(query).get();

    assertTrue("status field was not set", resp.hasStatus());
    assertEquals(expectFailure ? RequestStatus.FAILED : RequestStatus.OK, resp.getStatus());

    if (expectFailure) {
      assertEquals(errorType, resp.getError().getErrorType());
    }

    return resp.getPreparedStatement();
  }

  private static class ExpectedColumnResult {
    final String columnName;
    final String type;
    final boolean nullable;
    final int displaySize;
    final int precision;
    final int scale;
    final boolean signed;
    final String className;

    ExpectedColumnResult(String columnName, String type, boolean nullable, int displaySize, int precision, int scale,
        boolean signed, String className) {
      this.columnName = columnName;
      this.type = type;
      this.nullable = nullable;
      this.displaySize = displaySize;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
      this.className = className;
    }

    boolean isEqualsTo(ResultColumnMetadata result) {
      return
          result.getCatalogName().equals(InfoSchemaConstants.IS_CATALOG_NAME) &&
          result.getSchemaName().isEmpty() &&
          result.getTableName().isEmpty() &&
          result.getColumnName().equals(columnName) &&
          result.getLabel().equals(columnName) &&
          result.getDataType().equals(type) &&
          result.getIsNullable() == nullable &&
          result.getPrecision() == precision &&
          result.getScale() == scale &&
          result.getSigned() == signed &&
          result.getDisplaySize() == displaySize &&
          result.getClassName().equals(className) &&
          result.getSearchability() == ColumnSearchability.ALL &&
          !result.getAutoIncrement() &&
          !result.getCaseSensitivity() &&
          result.getUpdatability() == ColumnUpdatability.READ_ONLY &&
          result.getIsAliased() &&
          !result.getIsCurrency();
    }

    @Override
    public String toString() {
      return "ExpectedColumnResult[" +
          "columnName='" + columnName + '\'' +
          ", type='" + type + '\'' +
          ", nullable=" + nullable +
          ", displaySize=" + displaySize +
          ", precision=" + precision +
          ", scale=" + scale +
          ", signed=" + signed +
          ", className='" + className + '\'' +
          ']';
    }
  }

  private static String toString(ResultColumnMetadata metadata) {
    return "ResultColumnMetadata[" +
        "columnName='" + metadata.getColumnName() + '\'' +
        ", type='" + metadata.getDataType() + '\'' +
        ", nullable=" + metadata.getIsNullable() +
        ", displaySize=" + metadata.getDisplaySize() +
        ", precision=" + metadata.getPrecision() +
        ", scale=" + metadata.getScale() +
        ", signed=" + metadata.getSigned() +
        ", className='" + metadata.getClassName() + '\'' +
        ']';
  }

  private static void verifyMetadata(List<ExpectedColumnResult> expMetadata,
      List<ResultColumnMetadata> actMetadata) {
    assertEquals(expMetadata.size(), actMetadata.size());

    int i = 0;
    for(ExpectedColumnResult exp : expMetadata) {
      ResultColumnMetadata act = actMetadata.get(i++);

      assertTrue("Failed to find the expected column metadata: " + exp + ". Was: " + toString(act), exp.isEqualsTo(act));
    }
  }
}
