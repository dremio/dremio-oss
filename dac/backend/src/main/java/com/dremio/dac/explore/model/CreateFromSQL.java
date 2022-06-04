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
package com.dremio.dac.explore.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

/**
 * To create a new dataset from sql
 */
public class CreateFromSQL {

  private final String sql;
  private final List<String> context;
  private final Map<String, VersionContextReq> references;
  private final String engineName;

  public CreateFromSQL(String sql, List<String> context) {
    this(sql, context, null, null);
  }

  @VisibleForTesting
  public CreateFromSQL(String sql, List<String> context, Map<String, VersionContextReq> references) {
    this(sql, context, references, null);
  }

  @JsonCreator
  public CreateFromSQL(
    @JsonProperty("sql") String sql,
    @JsonProperty("context") List<String> context,
    @JsonProperty("references") Map<String, VersionContextReq> references,
    @JsonProperty("engineName") String engineName) {
    this.sql = sql;
    this.context = context;
    this.references = references;
    this.engineName = engineName;
  }

  public String getSql() {
    return sql;
  }

  public List<String> getContext() {
    return context;
  }

  public String getEngineName() {
    return engineName;
  }

  public Map<String, VersionContextReq> getReferences() {
    return references;
  }
}
