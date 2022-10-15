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
package com.dremio.jdbc.test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.dremio.common.util.TestTools;
import com.dremio.jdbc.Driver;
import com.dremio.jdbc.JdbcWithServerTestBase;
import com.google.common.base.Stopwatch;

public class JdbcTestActionBase extends JdbcWithServerTestBase {
  // Set a timeout unless we're debugging.
  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(40, TimeUnit.SECONDS);

  protected static final String WORKING_PATH;
  static {
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }

  protected void testQuery(final String sql) throws Exception {
    testAction(new JdbcAction() {

      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql);
        return r;
      }

    });
  }

  protected void testAction(JdbcAction action) throws Exception {
    testAction(action, -1);
  }

  protected void testAction(JdbcAction action, long rowcount) throws Exception {
    int rows = 0;
    Stopwatch watch = Stopwatch.createStarted();
    ResultSet r = action.getResult(getConnection());
    boolean first = true;
    while (r.next()) {
      rows++;
      ResultSetMetaData md = r.getMetaData();
      if (first) {
        for (int i = 1; i <= md.getColumnCount(); i++) {
          System.out.print(md.getColumnName(i));
          System.out.print('\t');
        }
        System.out.println();
        first = false;
      }

      for (int i = 1; i <= md.getColumnCount(); i++) {
        System.out.print(r.getObject(i));
        System.out.print('\t');
      }
      System.out.println();
    }

    System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));

    if (rowcount != -1) {
      Assert.assertEquals(rowcount, rows);
    }

    System.out.println("\n\n\n");

  }

  public static interface JdbcAction {
    ResultSet getResult(Connection c) throws SQLException;
  }

  static void resetConnection() throws Exception {
    JdbcWithServerTestBase.tearDownConnection();
    JdbcWithServerTestBase.setUpConnection();
  }

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetConnection();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

}
