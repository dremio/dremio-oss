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
package com.dremio.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class TestContextAwareCompletableFuture {

  private static final Context TEST_CONTEXT =
      Context.current().with(ContextKey.named("foo"), "bar");
  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  @AfterAll
  public static void tearDown() {
    EXECUTOR.shutdown();
  }

  @Test
  public void testCreateFromCompletableFuture() {
    CompletableFuture<String> dep = new CompletableFuture<>();
    ContextAwareCompletableFuture<String> contextAware =
        ContextAwareCompletableFuture.createFrom(dep, TEST_CONTEXT);
    dep.complete("foo");
    assertThat(contextAware.join()).isEqualTo("foo");
  }

  @Test
  public void testCreateFromContextAwareCompletableFuture() {
    ContextAwareCompletableFuture<String> dep = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
    ContextAwareCompletableFuture<String> contextAware =
        ContextAwareCompletableFuture.createFrom(dep, TEST_CONTEXT);
    assertThat(contextAware).isSameAs(dep);
  }

  @Test
  public void testThenApply() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = cf1.thenApply(s -> checkContextAndConcat(s, "bar"));
      CompletableFuture<String> cf3 = cf2.thenApply(s -> checkContextAndConcat(s, "baz"));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = cf1.thenApplyAsync(s -> checkContextAndConcat(s, "bar"));
      CompletableFuture<String> cf3 = cf2.thenApplyAsync(s -> checkContextAndConcat(s, "baz"));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = cf1.thenApplyAsync(s -> checkContextAndConcat(s, "bar"));
      CompletableFuture<String> cf3 =
          cf2.thenApplyAsync(s -> checkContextAndConcat(s, "baz"), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }
  }

  @Test
  public void testThenAccept() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 = cf1.thenAccept(s -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 =
          cf1.thenAcceptAsync(s -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 =
          cf1.thenAcceptAsync(s -> checkContextAndConcat(s, "bar", value), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }
  }

  @Test
  public void testThenRun() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 = cf1.thenRun(() -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      verify(cf2, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 = cf1.thenRunAsync(() -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      verify(cf2, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<Void> cf2 =
          cf1.thenRunAsync(() -> checkContextAndSet("bar", value), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, value, "bar");
    }
  }

  @Test
  public void testThenCombine() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 = new CompletableFuture<>();
      CompletableFuture<String> cf4 = cf1.thenCombine(cf2, this::checkContextAndConcat);
      CompletableFuture<String> cf5 = cf4.thenCombine(cf3, this::checkContextAndConcat);
      cf1.complete("foo");
      cf2.complete("bar");
      cf3.complete("baz");
      verify(cf4, cf5, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 = new CompletableFuture<>();
      CompletableFuture<String> cf4 = cf1.thenCombineAsync(cf2, this::checkContextAndConcat);
      CompletableFuture<String> cf5 = cf4.thenCombineAsync(cf3, this::checkContextAndConcat);
      cf1.complete("foo");
      cf2.complete("bar");
      cf3.complete("baz");
      verify(cf4, cf5, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 = new CompletableFuture<>();
      CompletableFuture<String> cf4 =
          cf1.thenCombineAsync(cf2, this::checkContextAndConcat, EXECUTOR);
      CompletableFuture<String> cf5 =
          cf4.thenCombineAsync(cf3, this::checkContextAndConcat, EXECUTOR);
      cf1.complete("foo");
      cf2.complete("bar");
      cf3.complete("baz");
      verify(cf4, cf5, "foo:bar:baz");
    }
  }

  @Test
  public void testThenAcceptBoth() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.thenAcceptBoth(cf2, (s1, s2) -> checkContextAndConcat(s1, s2, value));
      cf1.complete("foo");
      cf2.complete("bar");
      verify(cf3, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.thenAcceptBothAsync(cf2, (s1, s2) -> checkContextAndConcat(s1, s2, value));
      cf1.complete("foo");
      cf2.complete("bar");
      verify(cf3, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.thenAcceptBothAsync(cf2, (s1, s2) -> checkContextAndConcat(s1, s2, value), EXECUTOR);
      cf1.complete("foo");
      cf2.complete("bar");
      verify(cf3, value, "foo:bar");
    }
  }

  @Test
  public void testRunAfterBoth() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 = cf1.runAfterBoth(cf2, () -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      cf2.complete("foo");
      verify(cf3, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.runAfterBothAsync(cf2, () -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      cf2.complete("foo");
      verify(cf3, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.runAfterBothAsync(cf2, () -> checkContextAndSet("bar", value), EXECUTOR);
      cf1.complete("foo");
      cf2.complete("foo");
      verify(cf3, value, "bar");
    }
  }

  @Test
  public void testApplyToEither() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 = cf1.applyToEither(cf2, s -> checkContextAndConcat(s, "bar"));
      cf1.complete("foo");
      verify(cf3, "foo:bar");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 =
          cf1.applyToEitherAsync(cf2, s -> checkContextAndConcat(s, "bar"));
      cf1.complete("foo");
      verify(cf3, "foo:bar");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<String> cf3 =
          cf1.applyToEitherAsync(cf2, s -> checkContextAndConcat(s, "bar"), EXECUTOR);
      cf1.complete("foo");
      verify(cf3, "foo:bar");
    }
  }

  @Test
  public void testAcceptEither() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.acceptEither(cf2, s -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf3, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.acceptEitherAsync(cf2, s -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf3, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.acceptEitherAsync(cf2, s -> checkContextAndConcat(s, "bar", value), EXECUTOR);
      cf1.complete("foo");
      verify(cf3, value, "foo:bar");
    }
  }

  @Test
  public void testRunAfterEither() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 = cf1.runAfterEither(cf2, () -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      verify(cf3, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.runAfterEitherAsync(cf2, () -> checkContextAndSet("bar", value));
      cf1.complete("foo");
      verify(cf3, value, "bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = new CompletableFuture<>();
      CompletableFuture<Void> cf3 =
          cf1.runAfterEitherAsync(cf2, () -> checkContextAndSet("bar", value), EXECUTOR);
      cf1.complete("foo");
      verify(cf3, value, "bar");
    }
  }

  @Test
  public void testThenCompose() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.thenCompose(s -> checkContextAndConcat(s, "bar", CompletableFuture::supplyAsync));
      CompletableFuture<String> cf3 =
          cf2.thenCompose(s -> checkContextAndConcat(s, "baz", CompletableFuture::supplyAsync));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.thenComposeAsync(
              s -> checkContextAndConcat(s, "bar", CompletableFuture::supplyAsync));
      CompletableFuture<String> cf3 =
          cf2.thenComposeAsync(
              s -> checkContextAndConcat(s, "baz", CompletableFuture::supplyAsync));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.thenComposeAsync(
              s -> checkContextAndConcat(s, "bar", CompletableFuture::supplyAsync), EXECUTOR);
      CompletableFuture<String> cf3 =
          cf2.thenComposeAsync(
              s -> checkContextAndConcat(s, "baz", CompletableFuture::supplyAsync), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }
  }

  @Test
  public void testWhenComplete() {
    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.whenComplete((s, ex) -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.whenCompleteAsync((s, ex) -> checkContextAndConcat(s, "bar", value));
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }

    {
      AtomicReference<String> value = new AtomicReference<>();
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.whenCompleteAsync((s, ex) -> checkContextAndConcat(s, "bar", value), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, value, "foo:bar");
    }
  }

  @Test
  public void testHandle() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = cf1.handle((s, ex) -> checkContextAndConcat(s, "bar"));
      CompletableFuture<String> cf3 = cf2.handle((s, ex) -> checkContextAndConcat(s, "baz"));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 = cf1.handleAsync((s, ex) -> checkContextAndConcat(s, "bar"));
      CompletableFuture<String> cf3 = cf2.handleAsync((s, ex) -> checkContextAndConcat(s, "baz"));
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.handleAsync((s, ex) -> checkContextAndConcat(s, "bar"), EXECUTOR);
      CompletableFuture<String> cf3 =
          cf2.handleAsync((s, ex) -> checkContextAndConcat(s, "baz"), EXECUTOR);
      cf1.complete("foo");
      verify(cf2, cf3, "foo:bar:baz");
    }
  }

  @Test
  public void testExceptionally() {
    ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
    CompletableFuture<String> cf2 =
        cf1.exceptionally(
            ex -> {
              assertThat(Context.current()).isSameAs(TEST_CONTEXT);
              return "bar";
            });
    cf1.completeExceptionally(new RuntimeException());
    verify(cf2, "bar");
  }

  @Test
  public void testCompleteAsync() {
    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.completeAsync(
              () -> {
                assertThat(Context.current()).isSameAs(TEST_CONTEXT);
                return "bar";
              });
      verify(cf2, "bar");
    }

    {
      ContextAwareCompletableFuture<String> cf1 = new ContextAwareCompletableFuture<>(TEST_CONTEXT);
      CompletableFuture<String> cf2 =
          cf1.completeAsync(
              () -> {
                assertThat(Context.current()).isSameAs(TEST_CONTEXT);
                return "bar";
              },
              EXECUTOR);
      verify(cf2, "bar");
    }
  }

  private String checkContextAndConcat(String s1, String s2) {
    assertThat(Context.current()).isSameAs(TEST_CONTEXT);
    return s1 + ":" + s2;
  }

  private CompletionStage<String> checkContextAndConcat(
      String s1, String s2, Function<Supplier<String>, CompletionStage<String>> supplier) {
    assertThat(Context.current()).isSameAs(TEST_CONTEXT);
    return supplier.apply(() -> s1 + ":" + s2);
  }

  private void checkContextAndConcat(String s1, String s2, AtomicReference<String> value) {
    assertThat(Context.current()).isSameAs(TEST_CONTEXT);
    value.set(s1 + ":" + s2);
  }

  private void checkContextAndSet(String s, AtomicReference<String> value) {
    assertThat(Context.current()).isSameAs(TEST_CONTEXT);
    value.set(s);
  }

  private void verify(
      CompletableFuture<String> cf1, CompletableFuture<String> cf2, String expected) {
    assertThat(cf1).isInstanceOf(ContextAwareCompletableFuture.class);
    assertThat(cf2).isInstanceOf(ContextAwareCompletableFuture.class);
    assertThat(cf2.join()).isEqualTo(expected);
  }

  private void verify(CompletableFuture<String> cf, String expected) {
    assertThat(cf).isInstanceOf(ContextAwareCompletableFuture.class);
    assertThat(cf.join()).isEqualTo(expected);
  }

  private void verify(CompletableFuture<?> cf, AtomicReference<String> actual, String expected) {
    cf.join();
    assertThat(cf).isInstanceOf(ContextAwareCompletableFuture.class);
    assertThat(actual.get()).isEqualTo(expected);
  }
}
