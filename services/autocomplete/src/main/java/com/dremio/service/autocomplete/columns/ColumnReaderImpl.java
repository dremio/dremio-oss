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
package com.dremio.service.autocomplete.columns;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.exec.catalog.DremioCatalogReader;

/**
 * Implementation of ColumnReader that wraps DremioCatalogReader and extracts out the columns.
 */
public final class ColumnReaderImpl implements ColumnReader{
  private final DremioCatalogReader dremioCatalogReader;

  public ColumnReaderImpl(DremioCatalogReader dremioCatalogReader) {
    this.dremioCatalogReader = dremioCatalogReader;
  }

  @Override
  public Optional<Set<Column>> getColumnsForTableWithName(List<String> qualifiedName) {
    Optional<RelDataType> optionalTableSchema = this.dremioCatalogReader.getTableSchema(qualifiedName);
    if (!optionalTableSchema.isPresent()) {
      return Optional.empty();
    }

    RelDataType tableSchema = optionalTableSchema.get();
    List<RelDataTypeField> fields = tableSchema.getFieldList();
    Set<Column> columns = new HashSet<>();
    for (RelDataTypeField field : fields) {
      Column column = new Column(field.getName(), field.getType());
      columns.add(column);
    }

    return Optional.of(columns);
  }
}
