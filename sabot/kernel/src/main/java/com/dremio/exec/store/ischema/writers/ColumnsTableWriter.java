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

package com.dremio.exec.store.ischema.writers;

import java.util.Iterator;
import java.util.Set;

import com.dremio.exec.store.ischema.Column;
import com.dremio.exec.store.ischema.metadata.InformationSchemaMetadata;
import com.dremio.sabot.op.scan.OutputMutator;

public class ColumnsTableWriter extends TableWriter<Column> {

  private final String catalogNameOverride;

  public ColumnsTableWriter(
    Iterator<Column> messageIterator,
    Set<String> selectedFields,
    String catalogNameOverride
  ) {
    super(messageIterator, selectedFields);

    this.catalogNameOverride = catalogNameOverride;
  }

  @Override
  public void init(OutputMutator outputMutator) {
    addStringWriter(InformationSchemaMetadata.TABLE_CATALOG, outputMutator,
      catalogNameOverride != null ? ignored -> catalogNameOverride : column -> column.TABLE_CATALOG);
    addStringWriter(InformationSchemaMetadata.TABLE_SCHEMA, outputMutator, column -> column.TABLE_SCHEMA);
    addStringWriter(InformationSchemaMetadata.TABLE_NAME, outputMutator, column -> column.TABLE_NAME);
    addStringWriter(InformationSchemaMetadata.COLUMN_NAME, outputMutator, column -> column.COLUMN_NAME);
    addIntWriter(InformationSchemaMetadata.ORDINAL_POSITION, outputMutator, column -> column.ORDINAL_POSITION);
    addStringWriter(InformationSchemaMetadata.COLUMN_DEFAULT, outputMutator, column -> column.COLUMN_DEFAULT);
    addStringWriter(InformationSchemaMetadata.IS_NULLABLE, outputMutator, column -> column.IS_NULLABLE);
    addStringWriter(InformationSchemaMetadata.DATA_TYPE, outputMutator, column -> column.DATA_TYPE);
    addIntWriter(InformationSchemaMetadata.COLUMN_SIZE, outputMutator, column -> column.COLUMN_SIZE);
    addIntWriter(InformationSchemaMetadata.CHARACTER_MAXIMUM_LENGTH, outputMutator,
      column -> column.CHARACTER_MAXIMUM_LENGTH);
    addIntWriter(InformationSchemaMetadata.CHARACTER_OCTET_LENGTH, outputMutator,
      column -> column.CHARACTER_OCTET_LENGTH);
    addIntWriter(InformationSchemaMetadata.NUMERIC_PRECISION, outputMutator, column -> column.NUMERIC_PRECISION);
    addIntWriter(InformationSchemaMetadata.NUMERIC_PRECISION_RADIX, outputMutator,
      column -> column.NUMERIC_PRECISION_RADIX);
    addIntWriter(InformationSchemaMetadata.NUMERIC_SCALE, outputMutator, column -> column.NUMERIC_SCALE);
    addIntWriter(InformationSchemaMetadata.DATETIME_PRECISION, outputMutator, column -> column.DATETIME_PRECISION);
    addStringWriter(InformationSchemaMetadata.INTERVAL_TYPE, outputMutator, column -> column.INTERVAL_TYPE);
    addIntWriter(InformationSchemaMetadata.INTERVAL_PRECISION, outputMutator, column -> column.INTERVAL_PRECISION);
  }
}
