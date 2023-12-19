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
package com.dremio.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for LocalValue
 */
public class TestLocalValue {
  private static final LocalValue<String> stringLocalValue = new LocalValue<>();
  private static final LocalValue<Integer> intLocalValue = new LocalValue<>();

  @Before
  public void cleanup() {
    stringLocalValue.clear();
    intLocalValue.clear();
  }

  @Test
  public void testSet() throws Exception {
    stringLocalValue.set("value1");
    intLocalValue.set(1);
    assertEquals("value1", stringLocalValue.get().get());
    assertEquals(Integer.valueOf(1), intLocalValue.get().get());

    stringLocalValue.set("value2");
    intLocalValue.set(2);
    assertEquals("value2", stringLocalValue.get().get());
    assertEquals(Integer.valueOf(2), intLocalValue.get().get());

    try {
      intLocalValue.restore(null);
      fail("Restoring null should fail");
    } catch (Exception ignored) { }
  }

  @Test
  public void testClear() {
    stringLocalValue.set("value1");
    intLocalValue.set(1);

    stringLocalValue.clear();
    intLocalValue.clear();

    assertFalse(stringLocalValue.get().isPresent());
    assertFalse(intLocalValue.get().isPresent());
  }

  @Test
  public void testSaveRestore() {
    LocalValues saved = stringLocalValue.save();
    assertNotNull(saved);
    stringLocalValue.restore(saved);
    assertFalse(stringLocalValue.get().isPresent());
    assertFalse(intLocalValue.get().isPresent());

    stringLocalValue.set("value1");
    intLocalValue.set(1);

    saved = stringLocalValue.save();
    stringLocalValue.restore(saved);

    assertEquals("value1", stringLocalValue.get().get());
    assertEquals(Integer.valueOf(1), intLocalValue.get().get());
  }

  @Test
  public void testAsync() throws Exception {
    // Setting a LocalValue value i the current thread should not exist in a new thread
    stringLocalValue.set("value1");
    intLocalValue.set(1);

    final Runnable runnable1 = () -> {
      assertFalse(stringLocalValue.get().isPresent());

      stringLocalValue.set("value2");
      assertEquals("value2", stringLocalValue.get().get());
    };

    final Runnable runnable2 = () -> {
      assertFalse(stringLocalValue.get().isPresent());

      stringLocalValue.set("value3");
      assertEquals("value3", stringLocalValue.get().get());
    };

    final Future<Void> future1 = ThreadPerTaskExecutor.run(runnable1);
    final Future<Void> future2 = ThreadPerTaskExecutor.run(runnable2);

    future1.get();
    future2.get();

    assertEquals("value1", stringLocalValue.get().get());
  }

  @Test
  public void testLocalValueShouldNotLeakBetweenThreads() throws Exception {
    stringLocalValue.set("value1");
    intLocalValue.set(1);

    final LocalValues localValues = stringLocalValue.save();

    final Runnable runnable = () -> {
      assertFalse(stringLocalValue.get().isPresent());
      assertFalse(intLocalValue.get().isPresent());

      stringLocalValue.restore(localValues);
      assertEquals("value1", stringLocalValue.get().get());
      assertEquals(Integer.valueOf(1), intLocalValue.get().get());

      stringLocalValue.set("value2");
      assertEquals("value2", stringLocalValue.get().get());

      intLocalValue.set(2);
      assertEquals(Integer.valueOf(2), intLocalValue.get().get());
    };

    final Future<Void> future = ThreadPerTaskExecutor.run(runnable);
    future.get();

    assertEquals("value1", stringLocalValue.get().get());
    assertEquals(Integer.valueOf(1), intLocalValue.get().get());
  }

  @Test
  public void testLocalValuesAreImmutable() {
    final LocalValue.LocalValuesImpl initialLocalValues = (LocalValue.LocalValuesImpl) stringLocalValue.save();

    stringLocalValue.set("value1");
    intLocalValue.set(1);

    final LocalValue.LocalValuesImpl resultLocalValues = (LocalValue.LocalValuesImpl) stringLocalValue.save();

    assertFalse(Arrays.equals(resultLocalValues.get(), initialLocalValues.get()));
  }
}
