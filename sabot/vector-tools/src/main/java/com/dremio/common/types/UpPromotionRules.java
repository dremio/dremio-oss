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

import static com.dremio.common.collections.Tuple.of;
import static com.dremio.common.expression.CompleteType.BIGINT;
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;

import com.dremio.common.collections.Tuple;
import com.dremio.common.expression.CompleteType;

/**
 * Contains the list of the supported promotions between a filesystem source {@link CompleteType} and the destination
 * table {@link CompleteType}.
 */
public class UpPromotionRules {
  // A mapping between the (fileType, tableType) type, and the resultant CompleteType after up-promotion
  private static final Map<Tuple<CompleteType, CompleteType>, Function<CompleteType, CompleteType>> RULES = new HashMap<>();

  static {
    RULES.put(of(BIGINT, INT), fileType -> new CompleteType(new Int(64, true)));
    RULES.put(of(FLOAT, INT), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.SINGLE)));
    RULES.put(of(FLOAT, BIGINT), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.SINGLE)));
    RULES.put(of(DOUBLE, INT), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.DOUBLE)));
    RULES.put(of(DOUBLE, BIGINT), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.DOUBLE)));
    RULES.put(of(DOUBLE, FLOAT), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.DOUBLE)));
    RULES.put(of(DOUBLE, DECIMAL), fileType -> new CompleteType(new FloatingPoint(FloatingPointPrecision.DOUBLE)));
    RULES.put(of(DECIMAL, INT), fileType -> new CompleteType(new Decimal(fileType.getPrecision(), fileType.getScale())));
    RULES.put(of(DECIMAL, BIGINT), fileType -> new CompleteType(new Decimal(fileType.getPrecision(), fileType.getScale())));
    RULES.put(of(DECIMAL, FLOAT), fileType -> new CompleteType(new Decimal(fileType.getPrecision(), fileType.getScale())));
  }

  /**
   * Gets the up-promoted {@link CompleteType} from a table {@link CompleteType} and a file {@link CompleteType},
   * based on a pre-defined set of supported rules.
   *
   * @param fileType  {@link CompleteType} of the file
   * @param tableType {@link CompleteType} of the table
   * @return {@code Optional} of the resultant {@link CompleteType} if a match is found, {@code Optional.empty()} otherwise
   */
  public static Optional<CompleteType> getResultantType(CompleteType fileType, CompleteType tableType) {
    return Optional
      .ofNullable(RULES.get(of(fileType, tableType)))
      .map(function -> function.apply(fileType));
  }

  @VisibleForTesting
  static Map<Tuple<CompleteType, CompleteType>, Function<CompleteType, CompleteType>> getRules() {
    return Collections.unmodifiableMap(RULES);
  }
}
