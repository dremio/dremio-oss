/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

public class SchemaUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaUtilities.class);

  public static TableWithPath verify(final Catalog catalog, SqlIdentifier identifier){
    NamespaceKey path = catalog.resolveSingle(new NamespaceKey(identifier.names));
    DremioTable table = catalog.getTable(path);
    if(table == null) {
      throw UserException.parseError().message("Unable to find table %s.", path).build(logger);
    }

    return new TableWithPath(table);
  }

  public static class TableWithPath {

    private final DremioTable table;
    private final List<String> path;

    public TableWithPath(DremioTable table) {
      super();
      this.table = table;
      this.path = table.getPath().getPathComponents();
    }
    public DremioTable getTable() {
      return table;
    }
    public List<String> getPath() {
      return path;
    }

    public List<String> qualifyColumns(List<String> strings){
      final RelDataType type = table.getRowType(JavaTypeFactoryImpl.INSTANCE);
      return FluentIterable.from(strings).transform(new Function<String, String>(){

        @Override
        public String apply(String input) {
          RelDataTypeField field = type.getField(input, false, false);
          if(field == null){
            throw UserException.validationError()
              .message("Unable to find field %s in table %s. Available fields were: %s.",
                  input,
                  SqlUtils.quotedCompound(path),
                  FluentIterable.from(type.getFieldNames()).transform(SqlUtils.QUOTER).join(Joiner.on(", "))
                ).build(logger);
          }

          return field.getName();
        }

      }).toList();

    }


  }

}
