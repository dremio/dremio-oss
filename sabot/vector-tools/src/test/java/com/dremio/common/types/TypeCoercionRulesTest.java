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
import static com.dremio.common.expression.CompleteType.DATE;
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.expression.CompleteType.TIME;
import static com.dremio.common.expression.CompleteType.TIMESTAMP;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Test;

import com.dremio.common.expression.CompleteType;

public class TypeCoercionRulesTest {
  private static final CompleteType DEC_10_5 = new CompleteType(createDecimal(10, 5, null));
  private static final CompleteType DEC_18_18 = new CompleteType(createDecimal(18, 18, null));
  private static final CompleteType DEC_38_0 = new CompleteType(createDecimal(38, 0, null));
  private static final CompleteType DEC_100_50 = new CompleteType(createDecimal(100, 50, null));
  private static final TypeCoercionRules coercionRules = new TypeCoercionRules();

  @Test
  public void testGetResultantType() {
    assertThat(getType(INT, BIGINT)).isEqualTo(BIGINT);
    assertThat(getType(INT, FLOAT)).isEqualTo(DOUBLE);
    assertThat(getType(INT, DOUBLE)).isEqualTo(DOUBLE);

    assertThat(getType(INT, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(INT, DEC_18_18)).isEqualTo(DEC_18_18);
    assertThat(getType(INT, DEC_38_0)).isEqualTo(DEC_38_0);
    assertThat(getType(INT, DEC_10_5)).isEqualTo(DEC_10_5);

    assertThat(getType(BIGINT, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(BIGINT, DEC_18_18)).isEqualTo(DEC_18_18);
    assertThat(getType(BIGINT, DEC_38_0)).isEqualTo(DEC_38_0);
    assertThat(getType(BIGINT, DEC_10_5)).isEqualTo(DEC_10_5);

    assertThat(getType(FLOAT, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(FLOAT, DEC_18_18)).isEqualTo(DEC_18_18);
    assertThat(getType(FLOAT, DEC_38_0)).isEqualTo(DEC_38_0);
    assertThat(getType(FLOAT, DEC_10_5)).isEqualTo(DEC_10_5);

    assertThat(getType(INT, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(BIT, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(BIGINT, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(FLOAT, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(DOUBLE, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(DATE, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(TIME, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(TIMESTAMP, VARCHAR)).isEqualTo(VARCHAR);

    assertThat(getType(DECIMAL, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(DEC_18_18, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(DEC_38_0, VARCHAR)).isEqualTo(VARCHAR);
    assertThat(getType(DEC_10_5, VARCHAR)).isEqualTo(VARCHAR);

    assertThat(getType(FLOAT, DOUBLE)).isEqualTo(DOUBLE);
    assertThat(getType(BIGINT, FLOAT)).isEqualTo(DOUBLE);
    assertThat(getType(BIGINT, DOUBLE)).isEqualTo(DOUBLE);
    assertThat(getType(DECIMAL, DECIMAL)).isEqualTo(DECIMAL);

    assertThat(getType(DECIMAL, DOUBLE)).isEqualTo(DOUBLE);
    assertThat(getType(DEC_18_18, DOUBLE)).isEqualTo(DOUBLE);
    assertThat(getType(DEC_38_0, DOUBLE)).isEqualTo(DOUBLE);
    assertThat(getType(DEC_10_5, DOUBLE)).isEqualTo(DOUBLE);
  }

  @Test
  public void testAsymmetryOfRules() {
    assertThat(coercionRules.getResultantType(BIGINT, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(FLOAT, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DOUBLE, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DOUBLE, FLOAT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DOUBLE, BIGINT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DOUBLE, DECIMAL)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(FLOAT, BIGINT)).isEqualTo(Optional.empty());

    assertThat(coercionRules.getResultantType(DECIMAL, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_10_5, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_18_18, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_38_0, INT)).isEqualTo(Optional.empty());

    assertThat(coercionRules.getResultantType(DECIMAL, BIGINT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_10_5, BIGINT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_18_18, BIGINT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_38_0, BIGINT)).isEqualTo(Optional.empty());

    assertThat(coercionRules.getResultantType(DECIMAL, FLOAT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_10_5, FLOAT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_18_18, FLOAT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(DEC_38_0, FLOAT)).isEqualTo(Optional.empty());

    assertThat(coercionRules.getResultantType(VARCHAR, BIT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, BIGINT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, FLOAT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, DOUBLE)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, DATE)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, TIME)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, TIMESTAMP)).isEqualTo(Optional.empty());

    assertThat(coercionRules.getResultantType(VARCHAR, DECIMAL)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, DEC_10_5)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, DEC_18_18)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(VARCHAR, DEC_38_0)).isEqualTo(Optional.empty());
  }

  @Test
  public void testConversionBetweenDecimals() {
    assertThat(getType(DECIMAL, DEC_10_5)).isEqualTo(DEC_10_5);
    assertThat(getType(DECIMAL, DEC_18_18)).isEqualTo(DEC_18_18);
    assertThat(getType(DECIMAL, DEC_38_0)).isEqualTo(DEC_38_0);

    assertThat(getType(DEC_10_5, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(DEC_10_5, DEC_18_18)).isEqualTo(DEC_18_18);
    assertThat(getType(DEC_10_5, DEC_38_0)).isEqualTo(DEC_38_0);

    assertThat(getType(DEC_18_18, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(DEC_18_18, DEC_10_5)).isEqualTo(DEC_10_5);
    assertThat(getType(DEC_18_18, DEC_38_0)).isEqualTo(DEC_38_0);

    assertThat(getType(DEC_38_0, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(DEC_38_0, DEC_10_5)).isEqualTo(DEC_10_5);
    assertThat(getType(DEC_38_0, DEC_18_18)).isEqualTo(DEC_18_18);
  }

  @Test
  public void testErrorForComplexTypes() {
    assertThat(coercionRules.getResultantType(LIST, STRUCT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(STRUCT, LIST)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(STRUCT, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(INT, STRUCT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(LIST, INT)).isEqualTo(Optional.empty());
    assertThat(coercionRules.getResultantType(INT, LIST)).isEqualTo(Optional.empty());
  }

  @Test
  public void testUnsupportedDecimals() {
    expectException(() -> coercionRules.getResultantType(DEC_10_5, DEC_100_50));
    expectException(() -> coercionRules.getResultantType(DEC_100_50, DEC_10_5));
    expectException(() -> coercionRules.getResultantType(DEC_100_50, DEC_100_50));
  }

  private void expectException(Supplier<Optional<CompleteType>> supplier) {
    assertThatThrownBy(supplier::get)
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Max supported precision is 38");
  }

  private CompleteType getType(CompleteType fileType, CompleteType tableType) {
    return coercionRules.getResultantType(fileType, tableType).get();
  }
}
