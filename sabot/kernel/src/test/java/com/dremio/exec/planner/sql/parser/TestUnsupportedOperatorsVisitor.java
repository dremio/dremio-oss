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
package com.dremio.exec.planner.sql.parser;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.DremioSqlConformance;
import com.dremio.exec.planner.sql.parser.impl.ParserImpl;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.rpc.user.security.testing.UserServiceTestImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.sabot.rpc.user.UserSession;

public class TestUnsupportedOperatorsVisitor extends BaseTestQuery {

  private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder()
    .setCaseSensitive(false)
    .setConformance(DremioSqlConformance.INSTANCE)
    .setQuoting(Quoting.DOUBLE_QUOTE)
    .setParserFactory(ParserImpl.FACTORY).build();

  private static UnsupportedOperatorsVisitor visitor;

  @BeforeClass
  public static void beforeClass() {
    SabotContext context = getSabotContext();

    UserSession session = UserSession.Builder.newBuilder()
      .withSessionOptionManager(
        new SessionOptionManagerImpl(getSabotContext().getOptionValidatorListing()),
        getSabotContext().getOptionManager())
      .withUserProperties(UserProtos.UserProperties.getDefaultInstance())
      .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName(UserServiceTestImpl.ANONYMOUS).build())
      .setSupportComplexTypes(true)
      .build();

    final QueryContext queryContext = new QueryContext(session, context, UserBitShared.QueryId.getDefaultInstance());

    visitor = UnsupportedOperatorsVisitor.createVisitor(queryContext);
  }

  @Test
  public void testExtend() throws SqlParseException {
    try {
      String sql = "SELECT * FROM tbl EXTEND (b bigint)";
      visitor.visit((SqlSelect) SqlParser.create(sql, PARSER_CONFIG).parseQuery());
      fail(format("query (%s) should have failed", sql));
    } catch (UnsupportedOperationException ignored) {
      try {
        visitor.convertException();
        fail();
      } catch (SqlUnsupportedException e) {
        assertTrue(e.getMessage().contains("Dremio doesn't currently support EXTEND."));
      }
    }
  }

  @Test
  public void testExtendVariation() throws Exception {
    try {
      String sql = "SELECT * FROM tbl (b bigint)";
      visitor.visit((SqlSelect) SqlParser.create(sql, PARSER_CONFIG).parseQuery());
      fail(format("query (%s) should have failed", sql));
    } catch (UnsupportedOperationException ignored) {
      try {
        visitor.convertException();
        fail();
      } catch (SqlUnsupportedException e) {
        assertTrue(e.getMessage().contains("Dremio doesn't currently support EXTEND."));
      }
    }
  }
}
