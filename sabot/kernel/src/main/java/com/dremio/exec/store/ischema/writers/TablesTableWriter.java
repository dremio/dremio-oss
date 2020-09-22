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

import com.dremio.exec.store.ischema.metadata.InformationSchemaMetadata;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.catalog.Table;

/**
 * Writes "INFORMATION_SCHEMA"."TABLES" table.
 */
public class TablesTableWriter extends TableWriter<Table> {

  private final String catalogNameOverride;

  public TablesTableWriter(
    Iterator<Table> messageIterator,
    Set<String> selectedFields,
    String catalogNameOverride
  ) {
    super(messageIterator, selectedFields);
    this.catalogNameOverride = catalogNameOverride;
  }

  @Override
  public void init(OutputMutator outputMutator) {
    addStringWriter(InformationSchemaMetadata.TABLE_CATALOG, outputMutator,
      catalogNameOverride != null ? ignored -> catalogNameOverride : Table::getCatalogName);
    addStringWriter(InformationSchemaMetadata.TABLE_SCHEMA, outputMutator, Table::getSchemaName);
    addStringWriter(InformationSchemaMetadata.TABLE_NAME, outputMutator, Table::getTableName);
    addStringWriter(InformationSchemaMetadata.TABLE_TYPE, outputMutator, table -> table.getTableType().name());
  }
}
