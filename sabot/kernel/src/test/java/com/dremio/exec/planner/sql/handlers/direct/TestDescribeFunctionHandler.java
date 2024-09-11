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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlDescribeFunction;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestDescribeFunctionHandler {
  private DescribeFunctionHandler describeFunctionHandler;
  @Mock private UserDefinedFunctionCatalog userDefinedFunctionCatalog;
  @Mock private QueryContext queryContext;
  @Mock private UserSession userSession;

  private UserDefinedFunction udf1 =
      new UserDefinedFunction(
          "test1",
          "SELECT 1",
          CompleteType.VARCHAR,
          new ArrayList<>(),
          new ArrayList<>(),
          null,
          new Timestamp(System.currentTimeMillis()),
          new Timestamp(System.currentTimeMillis()));

  @Before
  public void setup() throws IOException {
    describeFunctionHandler = new DescribeFunctionHandler(queryContext);
    when(queryContext.getUserDefinedFunctionCatalog()).thenReturn(userDefinedFunctionCatalog);
    when(queryContext.getSession()).thenReturn(userSession);
    when(userSession.getSessionVersionForSource(any())).thenReturn(null);
    when(userDefinedFunctionCatalog.getFunction(any())).thenReturn(udf1);
  }

  @Test
  public void testToResult() throws Exception {
    SqlDescribeFunction describeFunction =
        new SqlDescribeFunction(
            SqlParserPos.ZERO,
            new SqlIdentifier(Collections.singletonList("test1"), SqlParserPos.ZERO),
            SqlTableVersionSpec.NOT_SPECIFIED);
    final List<DescribeFunctionHandler.DescribeResult> actualResults =
        describeFunctionHandler.toResult("foo", describeFunction);

    final List<DescribeFunctionHandler.DescribeResult> expectedResults =
        Lists.newArrayList(
            new DescribeFunctionHandler.DescribeResult(
                udf1.getName(),
                (udf1.getFunctionArgsList() != null) ? udf1.getFunctionArgsList().toString() : null,
                udf1.getReturnType().toString(),
                udf1.getFunctionSql(),
                udf1.getCreatedAt(),
                udf1.getModifiedAt(),
                null));

    assertEquals(expectedResults.size(), actualResults.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      verifyDescribeResult(expectedResults.get(i), actualResults.get(i));
    }
  }

  private void verifyDescribeResult(
      DescribeFunctionHandler.DescribeResult expected,
      DescribeFunctionHandler.DescribeResult actual) {
    assertEquals(expected.Name, actual.Name);
    assertEquals(expected.Input, actual.Input);
    assertEquals(expected.Body, actual.Body);
    assertEquals(expected.Created_At, actual.Created_At);
    assertEquals(expected.Last_Modified_At, actual.Last_Modified_At);
    assertEquals(expected.Owner, actual.Owner);
  }
}
