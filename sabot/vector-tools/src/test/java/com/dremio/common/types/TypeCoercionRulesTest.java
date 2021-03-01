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
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.expression.CompleteType.VARCHAR;
import static com.dremio.common.types.TypeCoercionRules.getResultantType;
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

public class TypeCoercionRulesTest {
  private static final CompleteType DEC_10_5 = new CompleteType(createDecimal(10, 5, null));
  private static final CompleteType DEC_18_18 = new CompleteType(createDecimal(18, 18, null));
  private static final CompleteType DEC_38_0 = new CompleteType(createDecimal(38, 0, null));
  private static final CompleteType DEC_100_50 = new CompleteType(createDecimal(100, 50, null));

  @Test
  public void testGetResultantType() {
    assertThat(getType(INT, BIGINT), is(BIGINT));
    assertThat(getType(INT, FLOAT), is(DOUBLE));
    assertThat(getType(INT, DOUBLE), is(DOUBLE));

    assertThat(getType(INT, DECIMAL), is(DECIMAL));
    assertThat(getType(INT, DEC_18_18), is(DEC_18_18));
    assertThat(getType(INT, DEC_38_0), is(DEC_38_0));
    assertThat(getType(INT, DEC_10_5), is(DEC_10_5));

    assertThat(getType(BIGINT, DECIMAL), is(DECIMAL));
    assertThat(getType(BIGINT, DEC_18_18), is(DEC_18_18));
    assertThat(getType(BIGINT, DEC_38_0), is(DEC_38_0));
    assertThat(getType(BIGINT, DEC_10_5), is(DEC_10_5));

    assertThat(getType(FLOAT, DECIMAL), is(DECIMAL));
    assertThat(getType(FLOAT, DEC_18_18), is(DEC_18_18));
    assertThat(getType(FLOAT, DEC_38_0), is(DEC_38_0));
    assertThat(getType(FLOAT, DEC_10_5), is(DEC_10_5));

    assertThat(getType(INT, VARCHAR), is(VARCHAR));
    assertThat(getType(BIT, VARCHAR), is(VARCHAR));
    assertThat(getType(BIGINT, VARCHAR), is(VARCHAR));
    assertThat(getType(FLOAT, VARCHAR), is(VARCHAR));
    assertThat(getType(DOUBLE, VARCHAR), is(VARCHAR));

    assertThat(getType(DECIMAL, VARCHAR), is(VARCHAR));
    assertThat(getType(DEC_18_18, VARCHAR), is(VARCHAR));
    assertThat(getType(DEC_38_0, VARCHAR), is(VARCHAR));
    assertThat(getType(DEC_10_5, VARCHAR), is(VARCHAR));

    assertThat(getType(FLOAT, DOUBLE), is(DOUBLE));
    assertThat(getType(BIGINT, FLOAT), is(DOUBLE));
    assertThat(getType(BIGINT, DOUBLE), is(DOUBLE));
    assertThat(getType(DECIMAL, DECIMAL), is(DECIMAL));

    assertThat(getType(DECIMAL, DOUBLE), is(DOUBLE));
    assertThat(getType(DEC_18_18, DOUBLE), is(DOUBLE));
    assertThat(getType(DEC_38_0, DOUBLE), is(DOUBLE));
    assertThat(getType(DEC_10_5, DOUBLE), is(DOUBLE));
  }

  @Test
  public void testAsymmetryOfRules() {
    assertThat(getResultantType(BIGINT, INT), is(Optional.empty()));
    assertThat(getResultantType(FLOAT, INT), is(Optional.empty()));
    assertThat(getResultantType(DOUBLE, INT), is(Optional.empty()));
    assertThat(getResultantType(DOUBLE, FLOAT), is(Optional.empty()));
    assertThat(getResultantType(DOUBLE, BIGINT), is(Optional.empty()));
    assertThat(getResultantType(DOUBLE, DECIMAL), is(Optional.empty()));
    assertThat(getResultantType(FLOAT, BIGINT), is(Optional.empty()));

    assertThat(getResultantType(DECIMAL, INT), is(Optional.empty()));
    assertThat(getResultantType(DEC_10_5, INT), is(Optional.empty()));
    assertThat(getResultantType(DEC_18_18, INT), is(Optional.empty()));
    assertThat(getResultantType(DEC_38_0, INT), is(Optional.empty()));

    assertThat(getResultantType(DECIMAL, BIGINT), is(Optional.empty()));
    assertThat(getResultantType(DEC_10_5, BIGINT), is(Optional.empty()));
    assertThat(getResultantType(DEC_18_18, BIGINT), is(Optional.empty()));
    assertThat(getResultantType(DEC_38_0, BIGINT), is(Optional.empty()));

    assertThat(getResultantType(DECIMAL, FLOAT), is(Optional.empty()));
    assertThat(getResultantType(DEC_10_5, FLOAT), is(Optional.empty()));
    assertThat(getResultantType(DEC_18_18, FLOAT), is(Optional.empty()));
    assertThat(getResultantType(DEC_38_0, FLOAT), is(Optional.empty()));

    assertThat(getResultantType(VARCHAR, BIT), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, BIGINT), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, FLOAT), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, DOUBLE), is(Optional.empty()));

    assertThat(getResultantType(VARCHAR, DECIMAL), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, DEC_10_5), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, DEC_18_18), is(Optional.empty()));
    assertThat(getResultantType(VARCHAR, DEC_38_0), is(Optional.empty()));
  }

  @Test
  public void testConversionBetweenDecimals() {
    assertThat(getType(DECIMAL, DEC_10_5), is(DEC_10_5));
    assertThat(getType(DECIMAL, DEC_18_18), is(DEC_18_18));
    assertThat(getType(DECIMAL, DEC_38_0), is(DEC_38_0));

    assertThat(getType(DEC_10_5, DECIMAL), is(DECIMAL));
    assertThat(getType(DEC_10_5, DEC_18_18), is(DEC_18_18));
    assertThat(getType(DEC_10_5, DEC_38_0), is(DEC_38_0));

    assertThat(getType(DEC_18_18, DECIMAL), is(DECIMAL));
    assertThat(getType(DEC_18_18, DEC_10_5), is(DEC_10_5));
    assertThat(getType(DEC_18_18, DEC_38_0), is(DEC_38_0));

    assertThat(getType(DEC_38_0, DECIMAL), is(DECIMAL));
    assertThat(getType(DEC_38_0, DEC_10_5), is(DEC_10_5));
    assertThat(getType(DEC_38_0, DEC_18_18), is(DEC_18_18));
  }

  @Test
  public void testErrorForComplexTypes() {
    assertThat(getResultantType(LIST, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, LIST), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(LIST, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, LIST), is(Optional.empty()));
  }

  @Test
  public void testUnsupportedDecimals() {
    expectException(() -> getResultantType(DEC_10_5, DEC_100_50));
    expectException(() -> getResultantType(DEC_100_50, DEC_10_5));
    expectException(() -> getResultantType(DEC_100_50, DEC_100_50));
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

  private CompleteType getType(CompleteType fileType, CompleteType tableType) {
    return getResultantType(fileType, tableType).get();
  }
}
