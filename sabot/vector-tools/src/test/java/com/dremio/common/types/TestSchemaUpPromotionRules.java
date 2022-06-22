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
import static com.dremio.common.expression.CompleteType.NULL;
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

public class TestSchemaUpPromotionRules {
  private static final CompleteType DEC_10_5 = new CompleteType(createDecimal(10, 5, null));
  private static final CompleteType DEC_18_18 = new CompleteType(createDecimal(18, 18, null));
  private static final CompleteType DEC_23_18 = new CompleteType(createDecimal(23, 18, null));
  private static final CompleteType DEC_38_0 = new CompleteType(createDecimal(38, 0, null));
  private static final CompleteType DEC_38_1 = new CompleteType(createDecimal(38, 1, null));
  private static final CompleteType DEC_38_33 = new CompleteType(createDecimal(38, 33, null));
  private static final CompleteType DEC_38_35 = new CompleteType(createDecimal(38, 35, null));
  private static final CompleteType DEC_100_50 = new CompleteType(createDecimal(100, 50, null));
  private static final SchemaUpPromotionRules upPromotionRules = new SchemaUpPromotionRules();

  @Test
  public void testGetResultantTypeApi() {
    assertThat(getType(BIGINT, INT)).isEqualTo(BIGINT);
    assertThat(getType(FLOAT, INT)).isEqualTo(DOUBLE);
    assertThat(getType(FLOAT, BIGINT)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, INT)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, BIGINT)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, FLOAT)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, DECIMAL)).isEqualTo(DOUBLE);
    assertThat(getType(VARCHAR, BIT)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, INT)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, BIGINT)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, FLOAT)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DOUBLE)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DECIMAL)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DEC_10_5)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DEC_18_18)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DEC_38_0)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, DATE)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, TIME)).isEqualTo(VARCHAR);
    assertThat(getType(VARCHAR, TIMESTAMP)).isEqualTo(VARCHAR);
    assertThat(getType(DECIMAL, INT)).isEqualTo(DECIMAL);
    assertThat(getType(DECIMAL, BIGINT)).isEqualTo(DECIMAL);
    assertThat(getType(DECIMAL, FLOAT)).isEqualTo(DECIMAL);
    assertThat(getType(INT, NULL)).isEqualTo(INT);
    assertThat(getType(BIGINT, NULL)).isEqualTo(BIGINT);
    assertThat(getType(FLOAT, NULL)).isEqualTo(FLOAT);
    assertThat(getType(DOUBLE, NULL)).isEqualTo(DOUBLE);
    assertThat(getType(VARCHAR, NULL)).isEqualTo(VARCHAR);
    assertThat(getType(DECIMAL, NULL)).isEqualTo(DECIMAL);
    assertThat(getType(TIMESTAMP, NULL)).isEqualTo(TIMESTAMP);
    assertThat(getType(TIME, NULL)).isEqualTo(TIME);
    assertThat(getType(DATE, NULL)).isEqualTo(DATE);
    assertThat(getType(BIT, NULL)).isEqualTo(BIT);
  }

  @Test
  public void testConversionFromDecimal() {
    assertThat(getType(DOUBLE, DEC_10_5)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, DEC_18_18)).isEqualTo(DOUBLE);
    assertThat(getType(DOUBLE, DEC_38_0)).isEqualTo(DOUBLE);

    assertThat(getType(DEC_10_5, INT)).isEqualTo(DEC_10_5);
    assertThat(getType(DEC_18_18, INT)).isEqualTo(DEC_18_18);
    assertThat(getType(DEC_38_0, INT)).isEqualTo(DEC_38_0);

    assertThat(getType(DEC_10_5, BIGINT)).isEqualTo(DEC_10_5);
    assertThat(getType(DEC_18_18, BIGINT)).isEqualTo(DEC_18_18);
    assertThat(getType(DEC_38_0, BIGINT)).isEqualTo(DEC_38_0);

    assertThat(getType(DEC_10_5, FLOAT)).isEqualTo(DEC_10_5);
    assertThat(getType(DEC_18_18, FLOAT)).isEqualTo(DEC_18_18);
    assertThat(getType(DEC_38_0, FLOAT)).isEqualTo(DEC_38_0);
  }

  @Test
  public void testConversionBetweenDecimalsWithoutPrecisionOverflow() {
    assertThat(getType(DEC_10_5, DEC_18_18)).isEqualTo(DEC_23_18);
    assertThat(getType(DECIMAL, DEC_18_18)).isEqualTo(DECIMAL);
    assertThat(getType(DEC_18_18, DECIMAL)).isEqualTo(DECIMAL);
    assertThat(getType(DEC_18_18, DEC_10_5)).isEqualTo(DEC_23_18);
  }

  @Test
  public void testConversionBetweenDecimalsWithPrecisionOverflow() {
    assertThat(getType(DECIMAL, DEC_10_5)).isEqualTo(DEC_38_33);
    assertThat(getType(DEC_38_35, DEC_38_1)).isEqualTo(DEC_38_1);
    assertThat(getType(DEC_18_18, DEC_38_0)).isEqualTo(DEC_38_0);
  }

  @Test
  public void testUnsupportedDecimals() {
    expectException(() -> upPromotionRules.getResultantType(DEC_10_5, DEC_100_50));
    expectException(() -> upPromotionRules.getResultantType(DEC_100_50, DEC_10_5));
    expectException(() -> upPromotionRules.getResultantType(DEC_100_50, DEC_100_50));
  }

  @Test
  public void testSomeUnsupportedTypes() {
    assertThat(upPromotionRules.getResultantType(LIST, STRUCT)).isEqualTo(Optional.empty());
    assertThat(upPromotionRules.getResultantType(STRUCT, LIST)).isEqualTo(Optional.empty());
    assertThat(upPromotionRules.getResultantType(STRUCT, INT)).isEqualTo(Optional.empty());
    assertThat(upPromotionRules.getResultantType(INT, STRUCT)).isEqualTo(Optional.empty());
    assertThat(upPromotionRules.getResultantType(LIST, INT)).isEqualTo(Optional.empty());
    assertThat(upPromotionRules.getResultantType(INT, LIST)).isEqualTo(Optional.empty());
  }

  private CompleteType getType(CompleteType fileType, CompleteType tableType) {
    return upPromotionRules.getResultantType(fileType, tableType).get();
  }

  private void expectException(Supplier<Optional<CompleteType>> supplier) {
    assertThatThrownBy(supplier::get)
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Max supported precision is 38");
  }
}
