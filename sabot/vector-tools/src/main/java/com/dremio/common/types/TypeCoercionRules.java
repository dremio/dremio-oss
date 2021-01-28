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
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;

import java.util.Optional;

import com.dremio.common.expression.CompleteType;

/**
 * Contains the list of the supported coercions between a filesystem source {@link CompleteType} and the destination
 * table {@link CompleteType}.
 */
public class TypeCoercionRules {
  /**
   * Gets the coerced {@link CompleteType} from a table {@link CompleteType} and a file {@link CompleteType},
   * based on a pre-defined set of supported rules.
   *
   * @param fileType  {@link CompleteType} of the file
   * @param tableType {@link CompleteType} of the table
   * @return {@code Optional} of the resultant {@link CompleteType} if a match is found, {@code Optional.empty()} otherwise
   */
  public static Optional<CompleteType> getResultantType(CompleteType fileType, CompleteType tableType) {
    if (fileType.equals(INT) && tableType.equals(BIGINT)) {
      return Optional.of(BIGINT);
    } else if (fileType.equals(FLOAT) && tableType.equals(DOUBLE)) {
      return Optional.of(DOUBLE);
    } else if (fileType.isDecimal() && tableType.isDecimal()) {
      //TODO [DX-27284]: Use the table type as the resultant until the right behavior is determined
      return Optional.of(tableType);
    } else {
      return Optional.empty();
    }
  }
}
