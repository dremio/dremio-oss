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
package com.dremio.exec.store.parquet2;

import java.util.Collection;
import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.GroupType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.options.OptionManager;
import com.dremio.exec.store.parquet2.WriterProvider.StructWriterProvider;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;

public class StructGroupConverter extends ParquetGroupConverter {

  private final StructWriter structWriter;
  private final WriterProvider writerProvider;

  public StructGroupConverter(
      OutputMutator mutator,
      StructWriter structWriter,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> arrowSchema,
      SchemaDerivationHelper schemaHelper) {
    super(
        mutator,
        schema,
        columns,
        options,
        arrowSchema,
        new Function<String, String>() {
          @Override
          public String apply(String input) {
            // each child has it's own name
            return input;
          }
        },
        schemaHelper);
    this.structWriter = structWriter;
    this.writerProvider = new StructWriterProvider(structWriter);

    convertChildren();
  }

  @Override
  WriterProvider getWriterProvider() {
    return writerProvider;
  }

  @Override
  public void start() {
    structWriter.start();
    super.startListWriters();
  }

  @Override
  public void end() {
    super.endListWriters();
    structWriter.end();
  }

}
