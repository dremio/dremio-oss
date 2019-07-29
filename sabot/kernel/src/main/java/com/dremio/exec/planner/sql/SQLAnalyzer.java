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
package com.dremio.exec.planner.sql;

import java.util.List;

import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class responsible for setting up dependencies required for SQL validation and suggestion
 * as well as execution of SQL validation and SQL error suggestion using Calcite's SqlAdvisor.
 */
public class SQLAnalyzer {

  @VisibleForTesting
  protected final SqlValidatorWithHints validator;

  protected SQLAnalyzer(final SqlValidatorWithHints validator) {
    this.validator = validator;
  }

  /**
   * Pass the SqlValidatorWithHints implementation to Calcite's SqlAdvisor
   * for query completion hints.
   *
   * @param sql             The SQL being evaluated.
   * @param cursorPosition  The current cursor position in the editor.
   * @return List<SqlMoniker> that represents the query completion options for the SQL editor.
   */
  public List<SqlMoniker> suggest(String sql, int cursorPosition) {
    SqlAdvisor sqlAdvisor = new SqlAdvisor(validator);
    String[] replaced = {null};
    return sqlAdvisor.getCompletionHints(sql, cursorPosition , replaced);
  }

  /**
   * Pass the SqlValidatorWithHints implementation to Calcite's SqlAdvisor
   * for query validation.
   *
   * @param sql             The SQL being evaluated.
   * @return List<SqlAdvisor.ValidateErrorInfo> that represents parser or validation errors.
   */
  public List<SqlAdvisor.ValidateErrorInfo> validate(String sql) {
    SqlAdvisor sqlAdvisor = new SqlAdvisor(validator);
    return sqlAdvisor.validate(sql);
  }
}
