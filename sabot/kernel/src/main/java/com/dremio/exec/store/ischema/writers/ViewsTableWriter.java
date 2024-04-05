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

import com.dremio.exec.store.ischema.metadata.InformationSchemaMetadata;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.catalog.View;
import java.util.Iterator;
import java.util.Set;

/** Writes "INFORMATION_SCHEMA"."VIEWS" table. */
public class ViewsTableWriter extends TableWriter<View> {

  private final String catalogNameOverride;

  public ViewsTableWriter(
      Iterator<View> messageIterator, Set<String> selectedFields, String catalogNameOverride) {
    super(messageIterator, selectedFields);

    this.catalogNameOverride = catalogNameOverride;
  }

  @Override
  public void init(OutputMutator outputMutator) {
    addStringWriter(
        InformationSchemaMetadata.TABLE_CATALOG,
        outputMutator,
        catalogNameOverride != null ? ignored -> catalogNameOverride : View::getCatalogName);
    addStringWriter(InformationSchemaMetadata.TABLE_SCHEMA, outputMutator, View::getSchemaName);
    addStringWriter(InformationSchemaMetadata.TABLE_NAME, outputMutator, View::getTableName);
    addStringWriter(
        InformationSchemaMetadata.VIEW_DEFINITION, outputMutator, View::getViewDefinition);
  }
}
