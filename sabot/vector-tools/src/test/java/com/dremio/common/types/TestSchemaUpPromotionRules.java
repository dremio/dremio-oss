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
import static com.dremio.common.types.SchemaUpPromotionRules.getResultantType;
import static org.apache.arrow.vector.types.pojo.ArrowType.Decimal.createDecimal;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.dremio.common.expression.CompleteType;

public class TestSchemaUpPromotionRules {
  @Test
  public void testGetResultantTypeApi() {
    assertThat(getType(BIGINT, INT), is(BIGINT));
    assertThat(getType(FLOAT, INT), is(FLOAT));
    assertThat(getType(FLOAT, BIGINT), is(FLOAT));
    assertThat(getType(DOUBLE, INT), is(DOUBLE));
    assertThat(getType(DOUBLE, BIGINT), is(DOUBLE));
    assertThat(getType(DOUBLE, FLOAT), is(DOUBLE));
    assertThat(getType(DOUBLE, DECIMAL), is(DOUBLE));
    assertThat(getType(DECIMAL, INT), is(DECIMAL));
    assertThat(getType(DECIMAL, BIGINT), is(DECIMAL));
    assertThat(getType(DECIMAL, FLOAT), is(DECIMAL));
  }

  @Test
  public void testConversionFromDecimal() {
    CompleteType dec105 = new CompleteType(createDecimal(10, 5, null));
    CompleteType dec1818 = new CompleteType(createDecimal(18, 18, null));
    CompleteType dec380 = new CompleteType(createDecimal(38, 0, null));
    CompleteType dec10050 = new CompleteType(createDecimal(100, 50, null));

    assertThat(getType(DOUBLE, dec105), is(DOUBLE));
    assertThat(getType(DOUBLE, dec1818), is(DOUBLE));
    assertThat(getType(DOUBLE, dec380), is(DOUBLE));
    assertThat(getType(DOUBLE, dec10050), is(DOUBLE));

    assertThat(getType(dec105, INT), is(dec105));
    assertThat(getType(dec1818, INT), is(dec1818));
    assertThat(getType(dec380, INT), is(dec380));
    assertThat(getType(dec10050, INT), is(dec10050));

    assertThat(getType(dec105, BIGINT), is(dec105));
    assertThat(getType(dec1818, BIGINT), is(dec1818));
    assertThat(getType(dec380, BIGINT), is(dec380));
    assertThat(getType(dec10050, BIGINT), is(dec10050));

    assertThat(getType(dec105, FLOAT), is(dec105));
    assertThat(getType(dec1818, FLOAT), is(dec1818));
    assertThat(getType(dec380, FLOAT), is(dec380));
    assertThat(getType(dec10050, FLOAT), is(dec10050));
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
}
