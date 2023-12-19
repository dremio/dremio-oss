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
import static com.dremio.exec.store.easy.EasyFormatUtils.isVarcharOptimizationPossible;
import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.tablefunctions.copyerrors.ValidationErrorRowWriter;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * Class is responsible for generating record batches for text file inputs. We generate
 * a record batch with a set of vectors of outgoing schema.
 */
class SchemaImposedOutput extends FieldTypeOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaImposedOutput.class);

  private final ExtendedFormatOptions extendedFormatOptions;
  private final BatchSchema validatedTableSchema;
  private final ValidationErrorRowWriter validationErrorWriter;
  private final OptionalInt errorColIndex;
  private boolean isWritingError = false;

  /**
   * We initialize and add the varchar vector for each incoming field in this
   * constructor.
   * @param outputMutator  Used to create/modify schema
   * @param fieldNames Incoming field names
   * @param sizeLimit Maximum size for an individual field
   * @param extendedFormatOptions format defining options
   * @param filePath path to the current processed input file
   * @throws SchemaChangeException
   */
  public SchemaImposedOutput(OutputMutator outputMutator, String[] fieldNames, int sizeLimit,
      ExtendedFormatOptions extendedFormatOptions,
      String filePath,
      boolean isValidationMode,
      BatchSchema validatedTableSchema,
      ValidationErrorRowWriter validationErrorWriter,
      boolean isOnErrorContinueMode)
      throws SchemaChangeException {
    super(sizeLimit, fieldNames.length);
    int totalFields = fieldNames.length;
    maxField = totalFields - 1;

    this.isValidationMode = isValidationMode;
    this.validatedTableSchema = validatedTableSchema;
    this.validationErrorWriter = validationErrorWriter;
    BatchSchema batchSchema = isValidationMode ? validatedTableSchema : outputMutator.getContainer().getSchema();
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
          throw new SchemaMismatchException(String.format("Duplicate column name %s. In file:  %s", fieldNames[i], filePath));
        }
        if (!isOnErrorContinueMode || !ColumnUtils.COPY_INTO_ERROR_COLUMN_NAME.equalsIgnoreCase(fieldNames[i])) {
          isAnyColumnMatched = true;
        }
        selectedFields[i] = true;
        vectors[i] = isValidationMode ? null : outputMutator.getVector(fieldNames[i]);
      }
    }

    if (!isAnyColumnMatched) {
      throw new SchemaMismatchException(String.format("No column name matches target %s in file %s", batchSchema, filePath));
    }

    this.errorColIndex = calculateErrorColIndex();
  }

  public SchemaImposedOutput(OutputMutator outputMutator, int sizeLimit,
      ExtendedFormatOptions extendedFormatOptions,
      boolean isValidationMode, BatchSchema validatedTableSchema,
      ValidationErrorRowWriter validationErrorWriter
      ) throws SchemaChangeException {
    super(sizeLimit, isValidationMode ? validatedTableSchema.getTotalFieldCount() :
      outputMutator.getContainer().getSchema().getTotalFieldCount());
    this.isValidationMode = isValidationMode;
    this.validatedTableSchema = validatedTableSchema;
    this.validationErrorWriter = validationErrorWriter;
    this.extendedFormatOptions = extendedFormatOptions;

    BatchSchema batchSchema = isValidationMode ? validatedTableSchema : outputMutator.getContainer().getSchema();
    int totalFields = batchSchema.getTotalFieldCount();
    this.maxField = totalFields - 1;
    for (int fieldIndex = 0; fieldIndex < totalFields; fieldIndex++) {
      String fieldName = batchSchema.getFields().get(fieldIndex).getName().toLowerCase();
      selectedFields[fieldIndex] = true;
      vectors[fieldIndex] = isValidationMode ? null : outputMutator.getVector(fieldName);
    }
    this.errorColIndex = calculateErrorColIndex();
  }

  @Override
  protected void writeValueInCurrentVector(int index, byte[] fieldBytes, int startIndex, int endIndex) {

    if (getErrorColIndex().orElse(Integer.MIN_VALUE) == currentFieldIndex && !isWritingError) {
      // in this case the input file contained more fields than what the target table schema has, and since we have +1 column
      // for errors, we need to be careful not to write those into the error column...
      return;
    }

    if (!isValidationMode && isVarcharOptimizationPossible(extendedFormatOptions, currentVector.getField().getType())) {
      // If we do not need to apply any string transformations and if our target field type is VARCHAR,
      // then we can skip converting to String type and directly write to currentValueVector
      if(currentDataPointer == 0 && extendedFormatOptions.getEmptyAsNull()) {
        // We will enter this block when the input string is empty AND we are required to treat empty strings as null.
        // Hence, write NULL to currentVector at position 'recordCount'
        ((VarCharVector) currentVector).setNull(recordCount);
      } else {
        ((VarCharVector) currentVector).setSafe(recordCount, fieldBytes, 0, currentDataPointer);
      }
    } else {
      String s = new String(fieldBytes, 0, currentDataPointer, StandardCharsets.UTF_8);
      if (!isValidationMode) {
        Object v = getValue(currentVector.getField(), s, extendedFormatOptions);
        writeToVector(currentVector, recordCount, v);
      } else {
        // no actual write, just type coercion
        getValue(validatedTableSchema.getFields().get(currentFieldIndex), s, extendedFormatOptions);
      }
    }
  }

  @Override
  public void finishBatch() {
    if (!isValidationMode) {
      super.finishBatch();
    }
  }

  /**
   * To be invoked instead of endField() in case the special error column is written.
   */
  void endErrorField() {
    isWritingError = true;
    try {
      super.endField();
    } finally {
      isWritingError = false;
    }
  }

  private OptionalInt calculateErrorColIndex() {
    return IntStream.range(0, vectors.length).filter(i -> vectors[i] != null)
      .filter(i -> ColumnUtils.COPY_INTO_ERROR_COLUMN_NAME.equalsIgnoreCase(vectors[i].getName())).findFirst();
  }

  public OptionalInt getErrorColIndex() {
    return errorColIndex;
  }

  public ExtendedFormatOptions getExtendedFormatOptions() {
    return extendedFormatOptions;
  }

  @SuppressWarnings("ReturnValueIgnored")
  void writeValidationError(long recordNumber, long linePosition, String error) {
    validationErrorWriter.write(
        validatedTableSchema.getFields().get(currentFieldIndex).getName(),
        recordNumber,
        linePosition,
        error);
  }
}
