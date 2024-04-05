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
package org.apache.calcite.sql.validate;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

/** DremioParameterScope */
public class DremioParameterScope extends EmptyScope {
  // ~ Instance fields --------------------------------------------------------

  /**
   * Map from the simple names of the parameters to types of the parameters ({@link RelDataType}).
   */
  private final Map<String, RelDataType> nameToTypeMap;

  // ~ Constructors -----------------------------------------------------------

  public DremioParameterScope(SqlValidatorImpl validator, Map<String, RelDataType> nameToTypeMap) {
    super(validator);
    this.nameToTypeMap = nameToTypeMap;
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    return SqlQualified.create(this, 1, null, identifier);
  }

  @Override
  public SqlValidatorScope getOperandScope(SqlCall call) {
    return this;
  }

  @Override
  public RelDataType resolveColumn(String name, SqlNode ctx) {
    return nameToTypeMap.get(name);
  }
}
