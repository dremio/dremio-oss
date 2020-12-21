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
import static com.dremio.common.expression.CompleteType.LIST;
import static com.dremio.common.expression.CompleteType.STRUCT;
import static com.dremio.common.types.UpPromotionRules.getResultantType;
import static com.dremio.common.types.UpPromotionRules.getRules;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.junit.Test;

import com.dremio.common.collections.Tuple;
import com.dremio.common.expression.CompleteType;

public class TestUpPromotionRules {
  /**
   * The purpose of this test is to ensure that {@link UpPromotionRules} contains only the rules that we want - no more
   * and no less. This test safeguards us against accidental changes to the list of rules. Any future changes to the
   * rule set should reflect in this test.
   */
  @Test
  public void testExactMatchOfSupportedTypes() {
    Map<Tuple<CompleteType, CompleteType>, Function<CompleteType, CompleteType>> supportedTypes = new HashMap<>(getRules());
    Function<CompleteType, CompleteType> function;

    function = supportedTypes.remove(of(BIGINT, INT));
    assertThat(getType(function, BIGINT), is(BIGINT));

    function = supportedTypes.remove(of(FLOAT, INT));
    assertThat(getType(function, FLOAT), is(FLOAT));

    function = supportedTypes.remove(of(FLOAT, BIGINT));
    assertThat(getType(function, FLOAT), is(FLOAT));

    function = supportedTypes.remove(of(DOUBLE, INT));
    assertThat(getType(function, DOUBLE), is(DOUBLE));

    function = supportedTypes.remove(of(DOUBLE, BIGINT));
    assertThat(getType(function, DOUBLE), is(DOUBLE));

    function = supportedTypes.remove(of(DOUBLE, FLOAT));
    assertThat(getType(function, DOUBLE), is(DOUBLE));

    function = supportedTypes.remove(of(DOUBLE, DECIMAL));
    assertThat(getType(function, DOUBLE), is(DOUBLE));

    function = supportedTypes.remove(of(DECIMAL, INT));
    assertThat(getType(function, DECIMAL), is(DECIMAL));

    function = supportedTypes.remove(of(DECIMAL, BIGINT));
    assertThat(getType(function, DECIMAL), is(DECIMAL));

    function = supportedTypes.remove(of(DECIMAL, FLOAT));
    assertThat(getType(function, DECIMAL), is(DECIMAL));

    // By now the supportedTypes map should be empty. This check ensures we don't have any more supported types than we expect
    assertThat(supportedTypes.size(), is(0));
  }

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
  public void testSomeUnsupportedTypes() {
    assertThat(getResultantType(LIST, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, LIST), is(Optional.empty()));
    assertThat(getResultantType(STRUCT, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, STRUCT), is(Optional.empty()));
    assertThat(getResultantType(LIST, INT), is(Optional.empty()));
    assertThat(getResultantType(INT, LIST), is(Optional.empty()));
  }

  private CompleteType getType(CompleteType bigint, CompleteType anInt) {
    return getResultantType(bigint, anInt).get();
  }

  private CompleteType getType(Function<CompleteType, CompleteType> function, CompleteType fileType) {
    return function.apply(fileType);
  }
}
