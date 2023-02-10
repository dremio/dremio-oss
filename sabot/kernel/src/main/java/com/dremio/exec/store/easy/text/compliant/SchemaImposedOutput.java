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
package com.dremio.exec.store.easy.text.compliant;

import static com.dremio.exec.store.easy.EasyFormatUtils.getValue;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of vectors of outgoing schema.
 */
class SchemaImposedOutput extends FieldTypeOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaImposedOutput.class);

  private final BatchSchema batchSchema;
  private ExtendedFormatOptions extendedFormatOptions;
  private OperatorContext context;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param columns  List of columns selected in the query
   * @param isStarQuery  boolean to indicate if all fields are selected or not
   * @param sizeLimit Maximum size for an individual field
   * @throws SchemaChangeException
   */
  public SchemaImposedOutput(OperatorContext context, OutputMutator outputMutator, String[] fieldNames, Collection<SchemaPath> columns, boolean isStarQuery, int sizeLimit, ExtendedFormatOptions extendedFormatOptions, String filePath) throws SchemaChangeException {
    super(sizeLimit, fieldNames.length);
    int totalFields = fieldNames.length;
    maxField = totalFields - 1;
    this.batchSchema = outputMutator.getContainer().getSchema();
    boolean isAnyColumnMatched = false;
    this.extendedFormatOptions = extendedFormatOptions;
    List<String> inputFields = batchSchema.getFields().stream()
            .map(Field::getName)
            .map(String::toLowerCase)
            .collect(Collectors.toList());
    Set<String> fieldNamesSet = new HashSet<>();
    for (int i = 0; i < totalFields; i++) {
      boolean selectField = inputFields.contains(fieldNames[i].toLowerCase());
      if (selectField) {
        if(!fieldNamesSet.contains(fieldNames[i].toLowerCase())) {
          fieldNamesSet.add(fieldNames[i].toLowerCase());
          if (logger.isDebugEnabled()) {
            logger.debug("'COPY INTO' Selecting field for reading : {}", fieldNames[i].toLowerCase());
          }
        } else {
          throw UserException.dataReadError().message(String.format("Duplicate column name %s. In file:  %s", fieldNames[i], filePath)).buildSilently();
        }
        isAnyColumnMatched = true;
        selectedFields[i] = true;
        vectors[i] = outputMutator.getVector(fieldNames[i]);
      }
    }

    if (!isAnyColumnMatched) {
      throw UserException.dataReadError().message("No column name matches target %s in file %s", batchSchema, filePath).buildSilently();
    }

    this.context = context;
  }
  @Override
  protected void writeValueInCurrentVector(int index, byte[] fieldBytes, int startIndex, int endIndex) {
    String s = new String(fieldBytes, 0, currentDataPointer, StandardCharsets.UTF_8);
    Object v = getValue(currentVector.getField(), s, extendedFormatOptions);
    writeToVector(currentVector, recordCount, v);
  }
}
