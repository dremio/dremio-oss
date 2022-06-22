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
package com.dremio.common.types;

import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.BIT;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.MAX_DECIMAL_PRECISION;
import static com.dremio.common.expression.CompleteType.NULL;
import static com.dremio.common.expression.CompleteType.VARCHAR;

import java.util.Optional;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import com.dremio.common.expression.CompleteType;

/**
 * Contains the list of the supported up-promotions for a table schema between a filesystem source {@link CompleteType}
 * and the destination table {@link CompleteType}.
 */
public class SchemaUpPromotionRules {
  /**
   * Gets the up-promoted {@link CompleteType} from a table {@link CompleteType} and a file {@link CompleteType},
   * based on a pre-defined set of supported rules.
   *
   * @param fileType  {@link CompleteType} of the file
   * @param tableType {@link CompleteType} of the table
   * @return {@code Optional} of the resultant {@link CompleteType} if a match is found, {@code Optional.empty()} otherwise
   */
  public Optional<CompleteType> getResultantType(CompleteType fileType, CompleteType tableType) {
    if (tableType.equals(NULL)) {
      return Optional.of(fileType);
    }
    if (fileType.equals(BIGINT)) {
      return getResultantTypeForBigIntFileType(tableType);
    }
    if (fileType.equals(FLOAT)) {
      return getResultantTypeForFloatFileType(tableType);
    }
    if (fileType.equals(DOUBLE)) {
      return getResultantTypeForDoubleFileType(tableType);
    }
    if (fileType.equals(VARCHAR)) {
      return getResultantTypeForVarcharFileType(tableType);
    }
    if (fileType.isValidDecimal()) {
      return getResultantTypeForDecimalFileType(fileType, tableType);
    }
    return Optional.empty();
  }

  private Optional<CompleteType> getResultantTypeForDecimalFileType(CompleteType fileType, CompleteType tableType) {
    if (tableType.equals(INT) || tableType.equals(BIGINT) || tableType.equals(FLOAT)) {
      Decimal decimal = Decimal.createDecimal(fileType.getPrecision(), fileType.getScale(), null);
      return Optional.of(new CompleteType(decimal));
    }
    if (tableType.isValidDecimal()) {
      int outputScale = computeFractional(fileType, tableType);
      int outputPrecision = computeIntegral(fileType, tableType) + outputScale;
      if (outputPrecision > MAX_DECIMAL_PRECISION) {
        return handlePrecisionOverflow(fileType, tableType);
      }
      return Optional.of(new CompleteType(Decimal.createDecimal(outputPrecision, outputScale, null)));
    }
    return Optional.empty();
  }

  private Optional<CompleteType> getResultantTypeForVarcharFileType(CompleteType tableType) {
    if (tableType.equals(BIT) || tableType.equals(INT) || tableType.equals(BIGINT) ||
      tableType.equals(FLOAT) || tableType.equals(DOUBLE) || tableType.isValidDecimal() ||
      tableType.isTemporal()) {
      return Optional.of(VARCHAR);
    }
    return Optional.empty();
  }

  private Optional<CompleteType> getResultantTypeForDoubleFileType(CompleteType tableType) {
    if (tableType.equals(INT) || tableType.equals(BIGINT) || tableType.equals(FLOAT) || tableType.isValidDecimal()) {
      return Optional.of(DOUBLE);
    }
    return Optional.empty();
  }

  private Optional<CompleteType> getResultantTypeForFloatFileType(CompleteType tableType) {
    if (tableType.equals(INT) || tableType.equals(BIGINT)) {
      return Optional.of(DOUBLE);
    }
    return Optional.empty();
  }

  private Optional<CompleteType> getResultantTypeForBigIntFileType(CompleteType tableType) {
    if (tableType.equals(INT)) {
      return Optional.of(BIGINT);
    }
    return Optional.empty();
  }

  private Optional<CompleteType> handlePrecisionOverflow(CompleteType fileType, CompleteType tableType) {
    // On precision overflow, use the max possible precision, and reduce the scale accordingly
    int outputPrecision = MAX_DECIMAL_PRECISION;
    int outputScale = outputPrecision - computeIntegral(fileType, tableType);
    return Optional.of(new CompleteType(Decimal.createDecimal(outputPrecision, outputScale, null)));
  }

  private int computeIntegral(CompleteType fileType, CompleteType tableType) {
    return Math.max(fileType.getPrecision() - fileType.getScale(), tableType.getPrecision() - tableType.getScale());
  }

  private int computeFractional(CompleteType fileType, CompleteType tableType) {
    return Math.max(fileType.getScale(), tableType.getScale());
  }
}
