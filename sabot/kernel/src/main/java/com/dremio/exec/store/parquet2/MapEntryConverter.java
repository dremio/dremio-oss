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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.store.parquet.ParquetColumnResolver;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.exec.store.parquet2.WriterProvider.MapWriterProvider;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Functions;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/** Second level of MAP conversion. Converts entry of a MAP */
public class MapEntryConverter extends ParquetGroupConverter {
  private final MapWriter mapWriter;
  private final long maxChildrenAllowed;
  private final String fieldName;
  private boolean isKey;
  private int childCount;

  public MapEntryConverter(
      ParquetColumnResolver columnResolver,
      String fieldName,
      OutputMutator mutator,
      MapWriter mapWriter,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> arrowSchema,
      SchemaDerivationHelper schemaHelper) {
    super(
        columnResolver,
        mutator,
        schema,
        columns,
        options,
        arrowSchema,
        Functions.identity(),
        schemaHelper);
    maxChildrenAllowed =
        schemaHelper.isLimitListItems()
            ? options.getOption(ExecConstants.PARQUET_LIST_ITEMS_THRESHOLD)
            : Integer.MAX_VALUE;
    childCount = 0;
    this.mapWriter = mapWriter;
    this.fieldName = fieldName;
    Type keyType = schema.getFields().get(0);
    Type valueType = schema.getFields().get(1);
    addChildConverter(
        fieldName, mutator, arrowSchema, columns.iterator(), keyType, Functions.identity());
    addChildConverter(
        fieldName, mutator, arrowSchema, columns.iterator(), valueType, Functions.identity());
  }

  @Override
  WriterProvider getWriterProvider() {
    isKey = !isKey;
    if (isKey) {
      return new MapWriterProvider(mapWriter.key());
    }
    return new MapWriterProvider(mapWriter.value());
  }

  @Override
  public void start() {
    childCount++;
    if (childCount > maxChildrenAllowed) {
      throw createListChildrenLimitException(fieldName, maxChildrenAllowed, childCount);
    }
    mapWriter.startEntry();
    written = true;
  }

  @Override
  public void end() {
    mapWriter.endEntry();
  }
}
