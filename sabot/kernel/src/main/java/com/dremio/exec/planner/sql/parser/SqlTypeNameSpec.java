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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * A <code>SqlTypeNameSpec</code> is a type name that allows to customize sql node unparsing and
 * data type deriving.
 *
 * <p>To customize sql node unparsing, override the method {@link #unparse(SqlWriter, int, int)}.
 */
public abstract class SqlTypeNameSpec extends SqlIdentifier {
  public SqlTypeNameSpec(String name, SqlParserPos pos) {
    super(name, pos);
  }

  public abstract RelDataType deriveType(RelDataTypeFactory typeFactory);

  public abstract RelDataType deriveType(SqlValidator sqlValidator);
}
