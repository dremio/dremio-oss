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
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * 2nd level of LOGICAL LIST conversion. Handles 'element'
 */
class LogicalListL2Converter extends ParquetGroupConverter {
  private static final Logger logger = LoggerFactory.getLogger(LogicalListL2Converter.class);

  private final WriterProvider writerProvider;

  LogicalListL2Converter(
      final WriterProvider writerProvider,
      OutputMutator mutator,
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
          public String apply(@Nullable String input) {
            return input; // should never be called
          }
        },
        schemaHelper);

    this.writerProvider = writerProvider;

    if (!isSupportedSchema(schema)) {
      throw UserException.dataReadError()
        .message("Unsupported LOGICAL LIST parquet schema")
        .addContext("schema", schema)
        .build(logger);
    }

    convertChildren();
  }

  private boolean isSupportedSchema(GroupType schema) {
    return schema.getFieldCount() == 1;
  }

  @Override
  WriterProvider getWriterProvider() {
    return writerProvider;
  }

  @Override
  protected void addChildConverter(OutputMutator mutator, List<Field> arrowSchema, Iterator<SchemaPath> colIterator, Type type, Function<String, String> childNameResolver) {
    final String nameForChild = "inner";
    if (type.isPrimitive()) {
      converters.add( getConverterForType(nameForChild, type.asPrimitiveType()));
    } else {
      final GroupType groupType = type.asGroupType();
      Collection<SchemaPath> c = Lists.newArrayList(colIterator);
      if (arrowSchema != null) {
        converters.add( groupConverterFromArrowSchema(nameForChild, "$data$", groupType, c));
      } else {
        converters.add( defaultGroupConverter(mutator, groupType, nameForChild, c, null));
      }
    }
  }

  @Override
  public void start() {}

  @Override
  public void end() {}
}
