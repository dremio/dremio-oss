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
package com.dremio.exec.store.parquet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.arrow.DremioArrowSchema;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Provides utility functions for {@link OutputMutator}.
 */
public class OutputMutatorHelper {
  public static void addFooterFieldsToOutputMutator(OutputMutator outputMutator, SchemaDerivationHelper schemaHelper,
                                                    MutableParquetMetadata footer, List<SchemaPath> columnsToRead) {
    Schema arrowSchema;
    try {
      arrowSchema = DremioArrowSchema.fromMetaData(footer.getFileMetaData().getKeyValueMetaData());
    } catch (Exception e) {
      arrowSchema = null;
    }

    final Set<String> columnsToReadSet = columnsToRead.stream()
      .map(col -> col.getRootSegment().getNameSegment().getPath().toLowerCase())
      .collect(Collectors.toSet());

    if (arrowSchema == null) {
      footer.getFileMetaData().getSchema().getFields().stream()
              .filter(field -> columnsToReadSet.contains(field.getName().toLowerCase()))
              .map(field -> ParquetTypeHelper.toField(field, schemaHelper))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .forEach(field -> outputMutator.addField(field, TypeHelper.getValueVectorClass(field)));
    } else {
      arrowSchema.getFields().stream()
              .filter(field -> columnsToReadSet.contains(field.getName().toLowerCase()))
              .forEach(field -> outputMutator.addField(field, TypeHelper.getValueVectorClass(field)));
    }

    outputMutator.getContainer().buildSchema();
    outputMutator.getAndResetSchemaChanged();
  }
}
