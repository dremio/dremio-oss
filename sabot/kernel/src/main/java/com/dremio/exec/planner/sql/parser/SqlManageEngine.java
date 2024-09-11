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

import java.util.Optional;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class SqlManageEngine extends SqlCall {

  protected final SqlIdentifier engineName;
  protected final SqlNumericLiteral minReplicas;
  protected final SqlNumericLiteral maxReplicas;

  public SqlManageEngine(
      SqlParserPos pos,
      SqlIdentifier engineName,
      SqlNumericLiteral minReplicas,
      SqlNumericLiteral maxReplicas) {
    super(pos);
    this.engineName = engineName;
    this.minReplicas = minReplicas;
    this.maxReplicas = maxReplicas;
  }

  public SqlIdentifier getEngineName() {
    return engineName;
  }

  public Optional<Integer> getMinReplicas() {
    return Optional.ofNullable(minReplicas).map(minReplicas -> minReplicas.intValue(true));
  }

  public Optional<Integer> getMaxReplicas() {
    return Optional.ofNullable(maxReplicas).map(maxReplicas -> maxReplicas.intValue(true));
  }
}
