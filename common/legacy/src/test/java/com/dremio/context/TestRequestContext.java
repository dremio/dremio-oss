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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Test;

/** Tests for request context */
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

    final FutureTask<Boolean> task =
        new FutureTask<>(
            () -> {
              assertEquals("map test", RequestContext.current().get(stringKey));
              assertEquals(Integer.valueOf(3), RequestContext.current().get(intKey));

              return true;
            });

    RequestContext.current().with(stringKey, "test").with(intKey, 1).with(map).run(task);

    final Boolean successful = task.get();
    assertTrue(successful);
  }

  @Test
  public void testWithout() {
    RequestContext context = RequestContext.empty().with(stringKey, "val123").with(intKey, 123);

    assertEquals("val123", context.get(stringKey));
    assertNull(context.without(stringKey).get(stringKey));
    assertEquals(Integer.valueOf(123), context.without(stringKey).get(intKey));
  }

  @Test
  public void testRunnable() throws Exception {
    final RequestContext context = RequestContext.current();

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));

    final FutureTask<Boolean> task =
        new FutureTask<>(
            () -> {
              assertEquals("test", RequestContext.current().get(stringKey));
              assertEquals(Integer.valueOf(2), RequestContext.current().get(intKey));

              RequestContext.current().with(stringKey, "test2");
              assertEquals("test", RequestContext.current().get(stringKey));

              return true;
            });

    RequestContext.current().with(stringKey, "test").with(intKey, 2).run(task);

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

    final Boolean successful =
        RequestContext.current()
            .with(stringKey, "test")
            .with(intKey, 2)
            .call(
                () -> {
                  assertEquals("test", RequestContext.current().get(stringKey));
                  assertEquals(Integer.valueOf(2), RequestContext.current().get(intKey));

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

    assertNull(context.get(stringKey));
    assertNull(context.get(intKey));

    final FutureTask<Boolean> task =
        new FutureTask<>(
            () -> {
              LOCAL_VALUE.set("somelocalvalue");
              return true;
            });

    LOCAL_VALUE.set("alocalvalue");

    RequestContext.current().with(stringKey, "test").with(intKey, 2).run(task);

    final Boolean successful = task.get();
    assertTrue(successful);

    // We changed another thread local and should expect the value to still be set
    assertEquals("somelocalvalue", LOCAL_VALUE.get().get());
  }

  @Test
  public void testCallableShouldPreserveOtherThreadLocalChanges() throws Exception {
    LOCAL_VALUE.set("alocalvalue");

    final Boolean successful =
        RequestContext.current()
            .with(stringKey, "test")
            .with(intKey, 2)
            .call(
                () -> {
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

  @Test
  public void testStreamForEach() {
    assertNull(currentString());

    List<String> data = new ArrayList<>();

    RequestContext.current()
        .with(stringKey, "testForEach")
        .run(() -> IntStream.of(1, 2, 3).forEach(x -> data.add(currentString() + x)));

    assertEquals(data, Arrays.asList("testForEach1", "testForEach2", "testForEach3"));

    assertNull(currentString());
  }

  @Test
  public void testStreamMap() throws Exception {
    assertNull(currentString());

    List<String> data =
        RequestContext.current()
            .with(stringKey, "testMap")
            .call(
                () ->
                    IntStream.of(1, 2, 3)
                        .boxed()
                        .map(x -> currentString() + x)
                        .collect(Collectors.toList()));

    assertEquals(data, Arrays.asList("testMap1", "testMap2", "testMap3"));

    assertNull(currentString());
  }

  @Test
  public void testStreamMapReturningStream() throws Exception {
    assertNull(currentString());

    // evaluating a stream outside the RequestContext loses its customization
    Stream<String> dataStream =
        RequestContext.current()
            .with(stringKey, "testReturnStreamBuggy")
            .call(() -> IntStream.of(1, 2, 3).boxed().map(x -> currentString() + x));

    List<String> data = dataStream.collect(Collectors.toList());
    assertEquals(data, Arrays.asList("null1", "null2", "null3"));

    assertNull(currentString());

    // pinning the context and using it inside the Stream.map fixes the problem
    // drawback: Stream generating logic needs to be aware of RequestContext
    // (not true when called by wrappers)
    dataStream =
        RequestContext.current()
            .with(stringKey, "testReturnStreamPinned")
            .call(
                () -> {
                  RequestContext pinnedCtx = RequestContext.current();
                  return IntStream.of(1, 2, 3).boxed().map(x -> pinnedCtx.get(stringKey) + x);
                });

    data = dataStream.collect(Collectors.toList());
    assertEquals(
        data,
        Arrays.asList(
            "testReturnStreamPinned1", "testReturnStreamPinned2", "testReturnStreamPinned3"));

    assertNull(currentString());

    // wrapping the stream inside RequestContext.callStream fixes the problem without the drawback
    List<String> lazilyPopulatedData = new ArrayList<>();
    dataStream =
        RequestContext.current()
            .with(stringKey, "testReturnWrappedStream")
            .callStream(
                () ->
                    IntStream.of(1, 2, 3)
                        .boxed()
                        .map(
                            x -> {
                              String res = currentString() + x;
                              lazilyPopulatedData.add(res);
                              return res;
                            })
                        .filter(x -> !x.endsWith("1")));

    assertNull(currentString());

    assertEquals("testReturnWrappedStream2", dataStream.findFirst().orElse(""));
    assertEquals(
        lazilyPopulatedData, Arrays.asList("testReturnWrappedStream1", "testReturnWrappedStream2"));

    assertNull(currentString());
  }

  @Test
  public void testCallStreamConcat() throws Exception {
    assertNull(currentString());

    Stream<String> dataStream1 =
        RequestContext.current()
            .with(stringKey, "first")
            .callStream(() -> IntStream.of(1, 2, 3).boxed().map(x -> currentString() + x));

    Stream<String> dataStream2 =
        RequestContext.current()
            .with(stringKey, "second")
            .callStream(() -> IntStream.of(1, 2, 3).boxed().map(x -> currentString() + x));

    List<String> data = Stream.concat(dataStream1, dataStream2).collect(Collectors.toList());
    assertEquals(
        data, Arrays.asList("first1", "first2", "first3", "second1", "second2", "second3"));

    assertNull(currentString());
  }

  @Test
  public void testCallStreamSamePipeline() throws Exception {
    assertNull(currentString());

    Stream<String> dataStream1 =
        RequestContext.current()
            .with(stringKey, "A")
            .callStream(() -> IntStream.of(1, 2, 3).boxed().map(x -> currentString() + x));

    Stream<String> dataStream2 =
        RequestContext.current()
            .with(stringKey, "B")
            .callStream(
                () ->
                    // this does not work because the existing pipeline is executing under "A"
                    dataStream1.map(x -> currentString() + x));

    Stream<String> dataStream3 =
        RequestContext.current()
            .with(stringKey, "C")
            .callStream(
                () -> {
                  RequestContext pinnedCtx = RequestContext.current();
                  return dataStream2.map(x -> pinnedCtx.callUnchecked(() -> currentString() + x));
                });

    List<String> data = dataStream3.collect(Collectors.toList());
    assertEquals(data, Arrays.asList("CAA1", "CAA2", "CAA3"));

    assertNull(currentString());
  }

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: test can be removed after DX-51884
  @Test
  public void testGuavaCache() throws Exception {
    assertNull(currentString());

    LoadingCache<Integer, String> cache =
        CacheBuilder.newBuilder()
            .maximumSize(1000) // items
            .softValues()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(CacheLoader.from((key) -> currentString() + key));

    List<String> data =
        RequestContext.current()
            .with(stringKey, "testGuava")
            .call(
                () ->
                    IntStream.of(1, 2, 3)
                        .boxed()
                        .map(x -> cache.getUnchecked(x))
                        .collect(Collectors.toList()));

    assertEquals(data, Arrays.asList("testGuava1", "testGuava2", "testGuava3"));

    assertNull(currentString());
  }

  @Test
  public void testCaffeineCache() throws Exception {
    assertNull(currentString());

    com.github.benmanes.caffeine.cache.LoadingCache<Integer, String> cache =
        Caffeine.newBuilder()
            .maximumSize(1000) // items
            .softValues()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(key -> currentString() + key);

    List<String> data =
        RequestContext.current()
            .with(stringKey, "testCaffeine")
            .call(
                () ->
                    IntStream.of(1, 2, 3)
                        .boxed()
                        .map(x -> cache.get(x))
                        .collect(Collectors.toList()));

    assertEquals(data, Arrays.asList("testCaffeine1", "testCaffeine2", "testCaffeine3"));

    assertNull(currentString());
  }

  private static String currentString() {
    return RequestContext.current().get(stringKey);
  }
}
