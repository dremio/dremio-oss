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

import static com.dremio.common.exceptions.FieldSizeLimitExceptionHelper.createListChildrenLimitException;

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
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.parquet.ParquetColumnResolver;
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
  private int childCount;
  private final long maxChildrenAllowed;
  private final String fieldName;

  LogicalListL2Converter(
      ParquetColumnResolver columnResolver,
      String fieldName,
      final WriterProvider writerProvider,
      OutputMutator mutator,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> arrowSchema,
      SchemaDerivationHelper schemaHelper) {
    super(
      columnResolver, mutator,
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

    this.fieldName = fieldName.split("\\.")[0];
    maxChildrenAllowed = schemaHelper.isLimitListItems() ?
     options.getOption(ExecConstants.PARQUET_LIST_ITEMS_THRESHOLD) : Integer.MAX_VALUE;
    childCount = 0;
    this.writerProvider = writerProvider;

    if (!isSupportedSchema(schema)) {
      throw UserException.dataReadError()
        .message("Unsupported LOGICAL LIST parquet schema")
        .addContext("schema: %s", schema)
        .build(logger);
    }

    convertChildren(fieldName);
  }

  private boolean isSupportedSchema(GroupType schema) {
    return schema.getFieldCount() == 1;
  }

  @Override
  WriterProvider getWriterProvider() {
    return writerProvider;
  }

  @Override
  protected void addChildConverter(String fieldName, OutputMutator mutator, List<Field> arrowSchema, Iterator<SchemaPath> colIterator, Type type, Function<String, String> childNameResolver) {
    final String nameForChild = "inner";
    // Column name to ID mapping creates child entry as 'columnName'.list.element
    // So, we will append 'list.element' so that name to ID matching works correctly
    final String fullChildName = fieldName.concat(".").concat("list.element");
    if (type.isPrimitive()) {
      converters.add( getConverterForType(fullChildName, type.asPrimitiveType()));
    } else {
      final GroupType groupType = type.asGroupType();
      Collection<SchemaPath> c = Lists.newArrayList(colIterator);
      if (arrowSchema != null) {
        converters.add( groupConverterFromArrowSchema(fullChildName, "$data$", groupType, c));
      } else {
        converters.add( defaultGroupConverter(fullChildName, mutator, groupType, c, null));
      }
    }
  }

  @Override
  public void start() {
    // list will have only one child.
    ParquetListElementConverter childConverter = (ParquetListElementConverter)converters.get(0);
    // signal start of writing an element
    childConverter.startElement();
    childCount++;
    if (childCount > maxChildrenAllowed) {
      throw createListChildrenLimitException(fieldName, maxChildrenAllowed);
    }
  }

  @Override
  public void end() {
    // list will have only one child. signal end of writing an element
    ParquetListElementConverter childConverter = (ParquetListElementConverter)converters.get(0);
    if(!childConverter.hasWritten()) {
      // if no element was written, then write null element
      childConverter.writeNullListElement();
    }
    // signal end of writing an element
    childConverter.endElement();
  }

  public void startList() {
    childCount = 0;
  }
}
