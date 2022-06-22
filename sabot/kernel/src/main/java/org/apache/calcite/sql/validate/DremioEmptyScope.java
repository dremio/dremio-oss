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

import java.util.List;

import com.google.common.collect.ImmutableList;

public class DremioEmptyScope extends EmptyScope {
  //~ Constructors -----------------------------------------------------------

  public DremioEmptyScope(SqlValidatorImpl validator) {
    super(validator);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void resolveTable(List<String> names,
      SqlNameMatcher nameMatcher,
      Path path, Resolved resolved) {
    SqlValidatorTable table = resolveTable(validator.catalogReader, names);
    if (null == table) {
      super.resolveTable(names, nameMatcher, path, resolved);
    } else {
      TableNamespace tableNamespace = new TableNamespace(validator, table);
      resolved.found(tableNamespace, false, null, path, ImmutableList.of());
    }
  }

  protected SqlValidatorTable resolveTable(SqlValidatorCatalogReader catalogReader, List<String> names){
    return catalogReader.getTable(names);
  }

  public static SqlValidatorScope createBaseScope(SqlValidatorImpl sqlValidator) {
    return new CatalogScope(
        new DremioEmptyScope(sqlValidator),
        ImmutableList.of("CATALOG")
    );
  }
}
