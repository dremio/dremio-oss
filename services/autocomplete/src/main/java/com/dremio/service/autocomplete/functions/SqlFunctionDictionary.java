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
package com.dremio.service.autocomplete.functions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlFunction;

/**
 * Dictionary of case insensitive function name to SqlFunction.
 */
public final class SqlFunctionDictionary {
  private final Map<String, SqlFunction> map;

  public SqlFunctionDictionary(List<SqlFunction> sqlFunctions) {
    Preconditions.checkNotNull(sqlFunctions);

    this.map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (SqlFunction sqlFunction : sqlFunctions) {
      this.map.put(sqlFunction.getName(), sqlFunction);
    }
  }

  public Optional<SqlFunction> tryGetValue(String functionName) {
    SqlFunction sqlFunction = this.map.get(functionName);
    if (sqlFunction == null) {
      return Optional.empty();
    }

    return Optional.of(sqlFunction);
  }

  public Collection<SqlFunction> getValues() {
    return this.map.values();
  }
  public Collection<String> getKeys() { return this.map.keySet(); }
}
