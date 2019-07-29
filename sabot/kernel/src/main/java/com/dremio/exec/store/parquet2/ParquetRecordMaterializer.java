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
package com.dremio.exec.store.parquet2;

import java.util.Collection;

import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.scan.OutputMutator;

public class ParquetRecordMaterializer extends RecordMaterializer<Void> {

  public StructGroupConverter root;
  private ComplexWriter complexWriter;

  public ParquetRecordMaterializer(OutputMutator mutator, ComplexWriter complexWriter, MessageType schema,
                                        Collection<SchemaPath> columns, OptionManager options, Schema arrowSchema,
                                        SchemaDerivationHelper schemaHelper) {
    this.complexWriter = complexWriter;
    root = new StructGroupConverter(
        mutator,
        complexWriter.rootAsStruct(),
        schema,
        columns,
        options,
        arrowSchema == null ? null : arrowSchema.getFields(),
        schemaHelper
    );
  }

  public void setPosition(int position) {
    complexWriter.setPosition(position);
  }

  @Override
  public Void getCurrentRecord() {
    return null;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
