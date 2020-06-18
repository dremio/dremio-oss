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
import com.dremio.service.catalog.Schema;

/**
 * Writes "INFORMATION_SCHEMA"."SCHEMATA" table.
 */
public class SchemataTableWriter extends TableWriter<Schema> {
  private final String catalogNameOverride;

  public SchemataTableWriter(
    Iterator<Schema> messageIterator,
    Set<String> selectedFields,
    String catalogNameOverride
  ) {
    super(messageIterator, selectedFields);

    this.catalogNameOverride = catalogNameOverride;
  }

  @Override
  public void init(OutputMutator outputMutator) {
    addStringWriter(InformationSchemaMetadata.CATALOG_NAME, outputMutator,
      catalogNameOverride != null ? ignored -> catalogNameOverride : Schema::getCatalogName);
    addStringWriter(InformationSchemaMetadata.SCHEMA_NAME, outputMutator, Schema::getSchemaName);
    addStringWriter(InformationSchemaMetadata.SCHEMA_OWNER, outputMutator, Schema::getSchemaOwner);
    addStringWriter(InformationSchemaMetadata.TYPE, outputMutator, schema -> schema.getSchemaType().name());
    addStringWriter(InformationSchemaMetadata.IS_MUTABLE, outputMutator,
      schema -> schema.getIsMutable() ? "YES" : "NO");
  }
}
