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
import static com.dremio.common.expression.CompleteType.DECIMAL;
import static com.dremio.common.expression.CompleteType.DOUBLE;
import static com.dremio.common.expression.CompleteType.FLOAT;
import static com.dremio.common.expression.CompleteType.INT;
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.types.TypeCoercionRules.getResultantType;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.dremio.common.expression.CompleteType;

public class TypeCoercionRulesTest {
  @Test
  public void testGetResultantType() {
    assertThat(getType(INT, BIGINT), is(BIGINT));
    assertThat(getType(FLOAT, DOUBLE), is(DOUBLE));
    assertThat(getType(DECIMAL, DECIMAL), is(DECIMAL));
  }

  @Test
  public void testAsymmetryOfRules() {
    assertThat(getResultantType(BIGINT, INT), is(Optional.empty()));
    assertThat(getResultantType(DOUBLE, FLOAT), is(Optional.empty()));
  }

  @Test
  public void testConversionBetweenDecimals() {
    CompleteType dec105 = new CompleteType(createDecimal(10, 5, null));
    CompleteType dec1818 = new CompleteType(createDecimal(18, 18, null));
    CompleteType dec380 = new CompleteType(createDecimal(38, 0, null));

    assertThat(getType(DECIMAL, dec105), is(dec105));
    assertThat(getType(DECIMAL, dec1818), is(dec1818));
    assertThat(getType(DECIMAL, dec380), is(dec380));

    assertThat(getType(dec105, DECIMAL), is(DECIMAL));
    assertThat(getType(dec105, dec1818), is(dec1818));
    assertThat(getType(dec105, dec380), is(dec380));

    assertThat(getType(dec1818, DECIMAL), is(DECIMAL));
    assertThat(getType(dec1818, dec105), is(dec105));
    assertThat(getType(dec1818, dec380), is(dec380));

    assertThat(getType(dec380, DECIMAL), is(DECIMAL));
    assertThat(getType(dec380, dec105), is(dec105));
    assertThat(getType(dec380, dec1818), is(dec1818));
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

  private CompleteType getType(CompleteType fileType, CompleteType tableType) {
    return getResultantType(fileType, tableType).get();
  }
}
