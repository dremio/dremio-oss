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

import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;

/** Factory for creating MockDremioTables. */
public final class MockDremioTableFactory {
  private MockDremioTableFactory() {}

  public static MockDremioTable createFromSchema(
      NamespaceKey namespaceKey,
      RelDataTypeFactory relDataTypeFactory,
      ImmutableList<MockSchemas.ColumnSchema> tableSchema) {
    Preconditions.checkNotNull(namespaceKey);
    Preconditions.checkNotNull(relDataTypeFactory);
    Preconditions.checkNotNull(tableSchema);

    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = relDataTypeFactory.builder();
    for (int index = 0; index < tableSchema.size(); index++) {
      MockSchemas.ColumnSchema columnSchema = tableSchema.get(index);

      RelDataType relDataType;
      if (columnSchema.getPrecision().isPresent()) {
        relDataType =
            relDataTypeFactory.createSqlType(
                columnSchema.getSqlTypeName(), columnSchema.getPrecision().get());
      } else {
        relDataType = relDataTypeFactory.createSqlType(columnSchema.getSqlTypeName());
      }

      relDataType = relDataTypeFactory.createTypeWithNullability(relDataType, true);

      RelDataTypeField field = new RelDataTypeFieldImpl(columnSchema.getName(), index, relDataType);

      fieldInfoBuilder.add(field);
    }

    RelDataType columns = fieldInfoBuilder.build();
    return MockDremioTable.create(namespaceKey, columns);
  }

  public static MockDremioTable createExpandingTable(
      NamespaceKey key, RelDataTypeFactory relDataTypeFactory) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(relDataTypeFactory);

    return MockDremioTable.create(key, new ExpandingRecordType(relDataTypeFactory));
  }
}
