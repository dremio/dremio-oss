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

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import java.util.Collection;
import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.parquet.ParquetReaderUtility;
import com.dremio.exec.store.parquet2.WriterProvider.ListWriterProvider;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;

/**
 * First level of LOGICAL LIST conversion. Handles 'list'
 */
class LogicalListL1Converter extends GroupConverter {
  private static final Logger logger = LoggerFactory.getLogger(LogicalListL1Converter.class);

  private final ListWriter listWriter;
  private final Converter converter;

  // This function assumes that the fields in the schema parameter are in the same order as the fields in the columns parameter. The
  // columns parameter may have fields that are not present in the schema, though.
  LogicalListL1Converter(
      final String listName,
      OutputMutator mutator,
      final WriterProvider writerProvider,
      GroupType schema,
      Collection<SchemaPath> columns,
      OptionManager options,
      List<Field> arrowSchema,
      ParquetReaderUtility.DateCorruptionStatus containsCorruptedDates,
      boolean readInt96AsTimeStamp) {

    listWriter = writerProvider.list(listName);

    if (!isSupportedSchema(schema)) {
      throw UserException.dataReadError()
        .message("Unsupported LOGICAL LIST parquet schema")
        .addContext("schema", schema)
        .build(logger);
    }

    final GroupType groupType = schema.getFields().get(0).asGroupType();
    converter = new LogicalListL2Converter(
      new ListWriterProvider(listWriter),
      mutator,
      groupType,
      columns,
      options,
      arrowSchema,
      containsCorruptedDates,
      readInt96AsTimeStamp);
  }

  /**
   * Checks if the schema is similar to the following:
   * <pre>
   * optional group <name> (LIST) {
   *   repeated group <list-name> {
   *     <element-repetition> <element-type> <element-name>;
   *   }
   * }
   * </pre>
   *
   * @param schema parquet group type
   * @return true is supported
   */
  static boolean isSupportedSchema(GroupType schema) {
    if (schema.getFieldCount() == 1) {
      Type type = schema.getType(0);
      // check: repeated group
      if (type.isPrimitive() || !type.isRepetition(REPEATED) || type.getOriginalType() != null) {
        return false;
      }
      return type.asGroupType().getFieldCount() == 1;
    }
    return false;
  }

  @Override
  public Converter getConverter(int i) {
    return converter;
  }

  @Override
  public void start() {
    listWriter.startList();
  }

  @Override
  public void end() {
    listWriter.endList();
  }
}
