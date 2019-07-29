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
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.GroupType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;

public class UnionGroupConverter extends ParquetGroupConverter {

  private final WriterProvider writerProvider;

  public UnionGroupConverter(
      OutputMutator mutator,
      WriterProvider writerProvider,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> children,
      final String name,
      SchemaDerivationHelper schemaHelper) {
    super(
        mutator,
        schema,
        columns,
        options,
        children,
        new Function<String, String>() {
          @Override
          public String apply(String input) {
            // all children have the same name but different types
            return name;
          }
        },
        schemaHelper
    );

    this.writerProvider = writerProvider;

    convertChildren();
  }

  @Override
  WriterProvider getWriterProvider() {
    return writerProvider;
  }

  @Override
  public void end() {
    super.startListWriters();
  }

  @Override
  public void start() {
    super.endListWriters();
  }

}
