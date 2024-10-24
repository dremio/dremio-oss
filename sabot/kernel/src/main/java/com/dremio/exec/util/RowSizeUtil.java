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
package com.dremio.exec.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.arrow.vector.types.Types.MinorType;

public class RowSizeUtil {
  // TODO: need to confirm all these values
  private static final int DEFAULT_TIMESTAMP_SIZE = 8;
  private static final int DEFAULT_TIME_SIZE = 8;
  private static final int DEFAULT_DATE_SIZE = 8;
  private static final int DEFAULT_INTERVAL_YEAR_SIZE = 4;
  private static final int DEFAULT_INTERVAL_DAY_SIZE = 8;
  private static final int DEFAULT_INTERVAL_MONTH_DAY_SIZE = 16;
  private static final int DEFAULT_BIT_SIZE = 1;
  private static final int DEFAULT_INT_SIZE = 4;
  private static final int DEFAULT_BIGINT_SIZE = 8;
  private static final int DEFAULT_FLOAT_SIZE = 4;
  private static final int DEFAULT_DOUBLE_SIZE = 8;

  public static int getFixedLengthTypeSize(MinorType type) {
    switch (type) {
      case TIMESTAMPMILLI:
        return DEFAULT_TIMESTAMP_SIZE;
      case TIMEMILLI:
        return DEFAULT_TIME_SIZE;
      case DATEMILLI:
        return DEFAULT_DATE_SIZE;
      case INTERVALDAY:
        return DEFAULT_INTERVAL_DAY_SIZE;
      case INTERVALYEAR:
        return DEFAULT_INTERVAL_YEAR_SIZE;
      case INTERVALMONTHDAYNANO:
        return DEFAULT_INTERVAL_MONTH_DAY_SIZE;
      case BIT:
        return DEFAULT_BIT_SIZE;
      case INT:
        return DEFAULT_INT_SIZE;
      case BIGINT:
        return DEFAULT_BIGINT_SIZE;
      case FLOAT4:
        return DEFAULT_FLOAT_SIZE;
      case FLOAT8:
        return DEFAULT_DOUBLE_SIZE;
    }
    throw new IllegalArgumentException("Unknown type: " + type);
  }

  public static int getFieldSizeForDecimalType(BigDecimal value) {
    BigInteger bigInteger = value.unscaledValue();
    byte[] byteArray = bigInteger.toByteArray();
    // Decimal size is BigInteger size + scale. Scale is stored as int.
    return byteArray.length + DEFAULT_INT_SIZE;
  }

  public static int getFieldSizeForDecimalType(int length) {
    // Decimal size is BigInteger size + scale. Scale is stored as int.
    return length + DEFAULT_INT_SIZE;
  }

  public static int getFieldSizeForVariableWidthType(byte[] value) {
    return value.length;
  }
}
