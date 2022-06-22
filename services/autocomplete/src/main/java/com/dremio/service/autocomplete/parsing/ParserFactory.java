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
package com.dremio.service.autocomplete.parsing;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;

public final class ParserFactory {
  private static final ParserConfig PARSER_CONFIG =  new ParserConfig(
    Quoting.DOUBLE_QUOTE,
    1000,
    true,
    PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT.getDefault().getBoolVal());

  private ParserFactory() {
  }

  public static SqlParser create(String sql) {
    return SqlParser.create(sql, PARSER_CONFIG);
  }
}
