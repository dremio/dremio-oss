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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.FutureTask;

import org.junit.Test;

/**
 * Tests for request context
 */
public class TestRequestContext {
  private static RequestContext.Key<String> stringKey = RequestContext.newKey("stringKey");
  private static RequestContext.Key<Integer> intKey = RequestContext.newKey("intKey");
  private static final LocalValue<String> LOCAL_VALUE = new LocalValue<>();

  @Test
  public void testWithMap() throws Exception {
    final RequestContext context = RequestContext.current();

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));

    final Map<RequestContext.Key<?>, Object> map = new HashMap<>();
    map.put(stringKey, "map test");
    map.put(intKey, 3);

    final FutureTask<Boolean> task = new FutureTask<>(() -> {
      assertEquals("map test", RequestContext.current().get(stringKey));
      assertEquals(new Integer(3), RequestContext.current().get(intKey));

      return true;
    });

    RequestContext.current()
      .with(stringKey, "test")
      .with(intKey, 1)
      .with(map)
      .run(task);

    final Boolean successful = task.get();
    assertTrue(successful);
  }

  @Test
  public void testRunnable() throws Exception {
    final RequestContext context = RequestContext.current();

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));

    final FutureTask<Boolean> task = new FutureTask<>(() -> {
      assertEquals("test", RequestContext.current().get(stringKey));
      assertEquals(new Integer(2), RequestContext.current().get(intKey));

      RequestContext.current().with(stringKey, "test2");
      assertEquals("test", RequestContext.current().get(stringKey));

      return true;
    });

    RequestContext.current()
      .with(stringKey, "test")
      .with(intKey, 2)
      .run(task);

    final Boolean successful = task.get();
    assertTrue(successful);

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));
  }

  @Test
  public void testCallable() throws Exception {
    final RequestContext context = RequestContext.current();

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));

    final Boolean successful = RequestContext.current()
      .with(stringKey, "test")
      .with(intKey, 2)
      .call(() -> {
        assertEquals("test", RequestContext.current().get(stringKey));
        assertEquals(new Integer(2), RequestContext.current().get(intKey));

        RequestContext.current().with(stringKey, "test2");
        assertEquals("test", RequestContext.current().get(stringKey));

        return true;
      });

    assertTrue(successful);

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));
  }

  @Test
  public void testRunnableShouldPreserveOtherThreadLocalChanges() throws Exception {
    final RequestContext context = RequestContext.current();

    final FutureTask<Boolean> task = new FutureTask<>(() -> {
      LOCAL_VALUE.set("somelocalvalue");
      return true;
    });

    LOCAL_VALUE.set("alocalvalue");

    RequestContext.current()
      .with(stringKey, "test")
      .with(intKey, 2)
      .run(task);

    final Boolean successful = task.get();
    assertTrue(successful);

    // We changed another thread local and should expect the value to still be set
    assertEquals("somelocalvalue", LOCAL_VALUE.get().get());
  }

  @Test
  public void testCallableShouldPreserveOtherThreadLocalChanges() throws Exception {
    LOCAL_VALUE.set("alocalvalue");

    final Boolean successful = RequestContext.current()
      .with(stringKey, "test")
      .with(intKey, 2)
      .call(() -> {
        LOCAL_VALUE.set("somelocalvalue");
        return true;
      });

    assertTrue(successful);

    // We changed another thread local and should expect the value to still be set
    assertEquals("somelocalvalue", LOCAL_VALUE.get().get());
  }

  @Test
  public void testKeyName() {
    assertEquals("stringKey", stringKey.getName());
    assertEquals("intKey", intKey.getName());
  }
}
