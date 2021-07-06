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
import static com.dremio.common.types.SchemaUpPromotionRules.getResultantType;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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

  @Test
  public void testGetResultantTypeApi() {
    assertThat(getType(BIGINT, INT), is(BIGINT));
    assertThat(getType(FLOAT, INT), is(DOUBLE));
    assertThat(getType(FLOAT, BIGINT), is(DOUBLE));
    assertThat(getType(DOUBLE, INT), is(DOUBLE));
    assertThat(getType(DOUBLE, BIGINT), is(DOUBLE));
    assertThat(getType(DOUBLE, FLOAT), is(DOUBLE));
    assertThat(getType(DOUBLE, DECIMAL), is(DOUBLE));
    assertThat(getType(VARCHAR, BIT), is(VARCHAR));
    assertThat(getType(VARCHAR, INT), is(VARCHAR));
    assertThat(getType(VARCHAR, BIGINT), is(VARCHAR));
    assertThat(getType(VARCHAR, FLOAT), is(VARCHAR));
    assertThat(getType(VARCHAR, DOUBLE), is(VARCHAR));
    assertThat(getType(VARCHAR, DECIMAL), is(VARCHAR));
    assertThat(getType(VARCHAR, DEC_10_5), is(VARCHAR));
    assertThat(getType(VARCHAR, DEC_18_18), is(VARCHAR));
    assertThat(getType(VARCHAR, DEC_38_0), is(VARCHAR));
    assertThat(getType(VARCHAR, DATE), is(VARCHAR));
    assertThat(getType(VARCHAR, TIME), is(VARCHAR));
    assertThat(getType(VARCHAR, TIMESTAMP), is(VARCHAR));
    assertThat(getType(DECIMAL, INT), is(DECIMAL));
    assertThat(getType(DECIMAL, BIGINT), is(DECIMAL));
    assertThat(getType(DECIMAL, FLOAT), is(DECIMAL));
    assertThat(getType(INT, NULL), is(INT));
    assertThat(getType(BIGINT, NULL), is(BIGINT));
    assertThat(getType(FLOAT, NULL), is(FLOAT));
    assertThat(getType(DOUBLE, NULL), is(DOUBLE));
    assertThat(getType(VARCHAR, NULL), is(VARCHAR));
    assertThat(getType(DECIMAL, NULL), is(DECIMAL));
    assertThat(getType(TIMESTAMP, NULL), is(TIMESTAMP));
    assertThat(getType(TIME, NULL), is(TIME));
    assertThat(getType(DATE, NULL), is(DATE));
    assertThat(getType(BIT, NULL), is(BIT));

  }

  @Test
  public void testConversionFromDecimal() {
    assertThat(getType(DOUBLE, DEC_10_5), is(DOUBLE));
    assertThat(getType(DOUBLE, DEC_18_18), is(DOUBLE));
    assertThat(getType(DOUBLE, DEC_38_0), is(DOUBLE));

    assertThat(getType(DEC_10_5, INT), is(DEC_10_5));
    assertThat(getType(DEC_18_18, INT), is(DEC_18_18));
    assertThat(getType(DEC_38_0, INT), is(DEC_38_0));

    assertThat(getType(DEC_10_5, BIGINT), is(DEC_10_5));
    assertThat(getType(DEC_18_18, BIGINT), is(DEC_18_18));
    assertThat(getType(DEC_38_0, BIGINT), is(DEC_38_0));

    assertThat(getType(DEC_10_5, FLOAT), is(DEC_10_5));
    assertThat(getType(DEC_18_18, FLOAT), is(DEC_18_18));
    assertThat(getType(DEC_38_0, FLOAT), is(DEC_38_0));
  }

  @Test
  public void testConversionBetweenDecimalsWithoutPrecisionOverflow() {
    assertThat(getType(DEC_10_5, DEC_18_18), is(DEC_23_18));
    assertThat(getType(DECIMAL, DEC_18_18), is(DECIMAL));
    assertThat(getType(DEC_18_18, DECIMAL), is(DECIMAL));
    assertThat(getType(DEC_18_18, DEC_10_5), is(DEC_23_18));
  }

  @Test
  public void testConversionBetweenDecimalsWithPrecisionOverflow() {
    assertThat(getType(DECIMAL, DEC_10_5), is(DEC_38_33));
    assertThat(getType(DEC_38_35, DEC_38_1), is(DEC_38_1));
    assertThat(getType(DEC_18_18, DEC_38_0), is(DEC_38_0));
  }

  @Test
  public void testUnsupportedDecimals() {
    expectException(() -> getResultantType(DEC_10_5, DEC_100_50));
    expectException(() -> getResultantType(DEC_100_50, DEC_10_5));
    expectException(() -> getResultantType(DEC_100_50, DEC_100_50));
  }

  @Test
  public void testSomeUnsupportedTypes() {
    assertThat(getResultantType(LIST, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, LIST), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(LIST, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, LIST), is(Optional.empty()));
  }

  private CompleteType getType(CompleteType fileType, CompleteType tableType) {
    return getResultantType(fileType, tableType).get();
  }

  private void expectException(Supplier<Optional<CompleteType>> supplier) {
    try {
      supplier.get();
      fail("Expected exception");
    } catch (Exception e) {
      assertThat(e, instanceOf(IllegalArgumentException.class));
      assertThat(e.getMessage(), containsString("Max supported precision is 38"));
    }
  }
}
