/*
 * Copyright (C) 2017 Dremio Corporation
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

  public SqlQuery(String sql, List<String> context, String username) {
    super();
    this.sql = sql;
    this.context = context;
    this.username = username;
  }

  public SqlQuery(String sql, List<String> context, SecurityContext securityContext) {
    super();
    this.sql = sql;
    this.context = context;
    this.username = securityContext.getUserPrincipal().getName();
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

  public SqlQuery(String sql, String username) {
    super();
    this.sql = sql;
    this.context = Collections.emptyList();
    this.username = username;
  }

  public SqlQuery cloneWithNewSql(String sql){
    return new SqlQuery(sql, context, username);
  }
}
