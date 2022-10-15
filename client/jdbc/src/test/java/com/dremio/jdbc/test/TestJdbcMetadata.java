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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;


public class TestJdbcMetadata extends JdbcTestActionBase {

  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(120, TimeUnit.SECONDS);

  @Test
  public void catalogs() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getCatalogs();
      }
    }, 1);
  }

  @Test
  public void allSchemas() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getSchemas();
      }
    });
  }

  @Test
  public void schemasWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getSchemas("DREMIO", "%INFO%");
      }
    }, 1);
  }

  @Test
  public void schemasWithUnderscores() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getSchemas("DREMIO", "INFORMATION\\_SCHEMA");
      }
    }, 1);
  }

  @Test
  public void allTables() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getTables(null, null, null, null);
      }
    });
  }

  @Test
  public void tablesWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getTables("DREMIO", "INFORMATION\\_SCHEMA", "CAT%", new String[]{"SYSTEM_TABLE", "SYSTEM_VIEW"});
      }
    }, 1);
  }

  @Test
  public void allColumns() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getColumns(null, null, null, null);
      }
    });
  }

  @Test
  public void columnsWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      @Override
      public ResultSet getResult(Connection c) throws SQLException {
        print(c.getMetaData().getColumns("DREMIO", "INFORMATION\\_SCHEMA", "CAT%", "%AME"));
        return c.getMetaData().getColumns("DREMIO", "INFORMATION\\_SCHEMA", "CAT%", "%AME");
      }
    }, 1);
  }
}
