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
package com.dremio.service.jobs;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.SecurityContext;

/**
 * Describes the minimum properties necessary to execute a query.
 */
public class SqlQuery {
  private final String sql;
  private final List<String> context;
  private final String username;
  private final String engineName;

  public SqlQuery(String sql, List<String> context, String username) {
    this.sql = sql;
    this.context = context;
    this.username = username;
    this.engineName = null;
  }

  public SqlQuery(String sql, List<String> context, String userName, String engineName) {
    this.sql = sql;
    this.context = context;
    this.username = userName;
    this.engineName = engineName;
  }

  public SqlQuery(String sql, List<String> context, SecurityContext securityContext) {
    this(sql, context, securityContext.getUserPrincipal().getName());
  }

  public SqlQuery(String sql, List<String> context, SecurityContext securityContext, String engineName) {
    this(sql, context, securityContext.getUserPrincipal().getName(), engineName);
  }

  public SqlQuery(String sql, String username) {
    this(sql, Collections.<String>emptyList(), username);
  }

  public String getSql() {
    return sql;
  }

  public List<String> getContext() {
    return context;
  }

  public String getUsername() {
    return username;
  }

  public SqlQuery cloneWithNewSql(String sql) {
    return new SqlQuery(sql, context, username);
  }

  public String getEngineName() {
    return engineName;
  }

  @Override
  public String toString() {
    return String.format("SqlQuery [sql=%s, context=%s, username=%s]", sql, context, username);
  }

}
