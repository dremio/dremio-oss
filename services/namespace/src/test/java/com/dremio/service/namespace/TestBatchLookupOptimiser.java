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
package com.dremio.service.namespace;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Unit test for BatchLookupOptimiser */
public class TestBatchLookupOptimiser {
  List<Integer> bulkLookup(List<String> keys) {
    List<Integer> values = new ArrayList<>();

    for (String key : keys) {
      Integer value = null;
      try {
        value = Integer.valueOf(key);
      } catch (NumberFormatException ignore) {
      }
      values.add(value);
    }
    return values;
  }

  @Test
  public void testSingleKey() {
    BatchLookupOptimiser<String, Integer> optimiser = new BatchLookupOptimiser<>(this::bulkLookup);

    optimiser.mayLookup("10");
    assertEquals((Integer) 10, optimiser.lookup("10"));
    assertEquals(0, optimiser.getNumMisses());
  }

  @Test
  public void testNullValue() {
    BatchLookupOptimiser<String, Integer> optimiser = new BatchLookupOptimiser<>(this::bulkLookup);

    optimiser.mayLookup("10.3");
    assertEquals((Integer) null, optimiser.lookup("10.3"));
    assertEquals(0, optimiser.getNumMisses());
  }

  @Test
  public void testBatch() {
    BatchLookupOptimiser<String, Integer> optimiser = new BatchLookupOptimiser<>(this::bulkLookup);

    List<String> inputs = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      inputs.add(Integer.toString(i));
    }
    for (String in : inputs) {
      optimiser.mayLookup(in);
    }
    for (String in : inputs) {
      assertEquals(Integer.valueOf(in), optimiser.lookup(in));
    }
    assertEquals(0, optimiser.getNumMisses());
  }

  @Test
  public void testMisses() {
    BatchLookupOptimiser<String, Integer> optimiser = new BatchLookupOptimiser<>(this::bulkLookup);

    List<String> inputs = new ArrayList<>();
    for (int i = 0; i < 100; ++i) {
      inputs.add(Integer.toString(i));
    }
    for (String in : inputs.subList(0, 50)) {
      optimiser.mayLookup(in);
    }
    for (String in : inputs) {
      assertEquals(Integer.valueOf(in), optimiser.lookup(in));
    }
    assertEquals(50, optimiser.getNumMisses());
  }
}
