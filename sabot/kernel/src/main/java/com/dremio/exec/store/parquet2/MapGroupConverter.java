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

import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.parquet.ParquetColumnResolver;
import com.dremio.exec.store.parquet.SchemaDerivationHelper;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Functions;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** First level of MAP conversion. */
public class MapGroupConverter extends ParquetGroupConverter {
  private static final Logger logger = LoggerFactory.getLogger(MapGroupConverter.class);
  private final MapWriter mapWriter;

  public MapGroupConverter(
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

    this.mapWriter = mapWriter;

    if (!isSupportedSchema(schema)) {
      throw UserException.dataReadError()
          .message("Unsupported MAP parquet schema for field: %s", fieldName)
          .addContext("schema: %s", schema)
          .build(logger);
    }
    GroupType entrySchema = schema.getFields().get(0).asGroupType();
    String entryFieldName = fieldName.concat(".").concat(entrySchema.getName());
    converters.add(
        new MapEntryConverter(
            columnResolver,
            entryFieldName,
            mutator,
            mapWriter,
            entrySchema,
            columns,
            options,
            arrowSchema != null && !arrowSchema.isEmpty()
                ? arrowSchema.get(0).getChildren()
                : arrowSchema,
            schemaHelper));
  }

  /**
   * Checks if the schema is similar to the following:
   *
   * <pre>
   * optional group <name> (MAP) {
   *   repeated group <map-name> {
   *    required key;
   *    optional value;
   *   }
   * }
   * </pre>
   *
   * @param schema parquet group type
   * @return true is supported
   */
  private boolean isSupportedSchema(GroupType schema) {
    if (schema.getFieldCount() == 1) {
      Type type = schema.getType(0);
      // check: repeated group
      if (type.isPrimitive()
          || !type.isRepetition(REPEATED)
          || type.asGroupType().getFieldCount() != 2) {
        return false;
      }
      Type keyType = type.asGroupType().getType(0);
      return keyType.isRepetition(REQUIRED);
    }
    return false;
  }

  @Override
  WriterProvider getWriterProvider() {
    return null;
  }

  @Override
  public void start() {
    mapWriter.startMap();
    written = true;
  }

  @Override
  public void end() {
    mapWriter.endMap();
  }

  @Override
  public void writeNullListElement() {
    mapWriter.writeNull();
  }
}
