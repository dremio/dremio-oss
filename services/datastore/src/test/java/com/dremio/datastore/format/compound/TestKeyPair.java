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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test KeyPair class.
 */
@RunWith(Parameterized.class)
public class TestKeyPair {

  @Parameterized.Parameters(name = "Values: {0}, Valid: {1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {Collections.emptyList(), false},
      {Collections.singletonList("value1"), false},
      {Arrays.asList("value1", "value2"), true},
      {Arrays.asList("value1", "value2", "value3"), false},
    });
  }

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private final List<Object> values;
  private final boolean valid;

  public TestKeyPair(List<Object> values, boolean valid) {
    this.values = values;
    this.valid = valid;
  }

  @Test
  public void testOf() {
    //setup expected exception
    if (!valid) {
      thrown.expect(IllegalArgumentException.class);
    }

    KeyPair<String, String> keyPair = KeyPair.of(values);

    assertNotNull(keyPair);
    assertEquals(values.get(0), keyPair.getKey1());
    assertEquals(values.get(1), keyPair.getKey2());
    assertEquals(values, keyPair);
  }
}
