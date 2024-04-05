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

package com.dremio.exec.tablefunctions.copyerrors;

import static com.dremio.exec.store.iceberg.IcebergUtils.writeToVector;

import java.util.function.IntSupplier;
import org.apache.arrow.vector.ValueVector;

/**
 * Utility class for writing one row of validation error into result vectors. Schema should match
 * that of {@link CopyErrorsTranslatableTable#COPY_ERRORS_TABLEFUNCTION_SCHEMA}
 */
@FunctionalInterface
public interface ValidationErrorRowWriter {

  static ValidationErrorRowWriter newVectorWriter(
      ValueVector[] validationResult,
      String filePath,
      String originalJobId,
      IntSupplier batchPositionSupplier) {
    return (fieldName, recordNumber, linePosition, error) -> {
      int batchPosition = batchPositionSupplier.getAsInt();
      writeToVector(validationResult[0], batchPosition, originalJobId);
      writeToVector(validationResult[1], batchPosition, filePath);
      writeToVector(validationResult[2], batchPosition, linePosition);
      writeToVector(validationResult[3], batchPosition, recordNumber);
      writeToVector(validationResult[4], batchPosition, fieldName);
      writeToVector(validationResult[5], batchPosition, error);
    };
  }

  void write(String fieldName, Long recordNumber, Long linePosition, String error);
}
