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
package com.dremio.datastore.format.compound;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test KeyTriple class.
 */
@RunWith(Parameterized.class)
public class TestKeyTriple {

  @Parameterized.Parameters(name = "Values: {0}, Valid: {1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {Collections.emptyList(), false},
      {Collections.singletonList("value1"), false},
      {Arrays.asList("value1", "value2"), false},
      {Arrays.asList("value1", "value2", "value3", "value4"), false},
      {Arrays.asList("value1", "value2", "value3"), true}
    });
  }

  private final List<Object> values;
  private final boolean valid;

  public TestKeyTriple(List<Object> values, boolean valid) {
    this.values = values;
    this.valid = valid;
  }

  @Test
  public void testOf() {
    if (!valid) {
      assertThatThrownBy(() -> KeyTriple.of(values))
        .isInstanceOf(IllegalArgumentException.class);
    } else {
      final KeyTriple<String, String, String> keyPair = KeyTriple.of(values);

      assertNotNull(keyPair);
      assertEquals(values.get(0), keyPair.getKey1());
      assertEquals(values.get(1), keyPair.getKey2());
      assertEquals(values.get(2), keyPair.getKey3());
      assertEquals(values, keyPair);
    }
  }
}
