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

import io.opentelemetry.context.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** {@link CompletableFuture} implementation that propagates context to its dependents */
public class ContextAwareCompletableFuture<T> extends CompletableFuture<T> {

  private final Context context;

  public ContextAwareCompletableFuture() {
    this.context = Context.current();
  }

  public ContextAwareCompletableFuture(Context context) {
    this.context = context;
  }

  private ContextAwareCompletableFuture(Context context, CompletionStage<T> dep) {
    this.context = context;

    dep.whenComplete(
        (result, error) -> {
          if (error == null) {
            this.complete(result);
          } else {
            this.completeExceptionally(error);
          }
        });
  }

  /**
   * Creates a {@link ContextAwareCompletableFuture} instance based on the current {@link Context}
   * that will complete when the provided dependency completes. If the provided dependency action is
   * already an instance of {@link ContextAwareCompletableFuture} with the same context, it will
   * simply return it.
   */
  public static <U> ContextAwareCompletableFuture<U> createFrom(CompletionStage<U> dep) {
    return createFrom(dep, Context.current());
  }

  /**
   * Creates a {@link ContextAwareCompletableFuture} instance based on the provided {@link Context}
   * that will complete when the provided dependency completes. If the provided dependency action is
   * already an instance of {@link ContextAwareCompletableFuture} with the same context, it will
   * simply return it.
   */
  public static <U> ContextAwareCompletableFuture<U> createFrom(
      CompletionStage<U> dep, Context context) {
    if (dep instanceof ContextAwareCompletableFuture) {
      ContextAwareCompletableFuture<U> contextAware = (ContextAwareCompletableFuture<U>) dep;
      if (contextAware.context == context) {
        return contextAware;
      }
    }
    return new ContextAwareCompletableFuture<>(context, dep);
  }

  @Override
  public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
    return super.thenApply(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
    return super.thenApplyAsync(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenApplyAsync(
      Function<? super T, ? extends U> fn, Executor executor) {
    return super.thenApplyAsync(context.wrapFunction(fn), executor);
  }

  @Override
  public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
    return super.thenAccept(context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
    return super.thenAcceptAsync(context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
    return super.thenAcceptAsync(context.wrapConsumer(action), executor);
  }

  @Override
  public CompletableFuture<Void> thenRun(Runnable action) {
    return super.thenRun(context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action) {
    return super.thenRunAsync(context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
    return super.thenRunAsync(context.wrap(action), executor);
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombine(
      CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return super.thenCombine(other, context.wrapFunction(fn));
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(
      CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
    return super.thenCombineAsync(other, context.wrapFunction(fn));
  }

  @Override
  public <U, V> CompletableFuture<V> thenCombineAsync(
      CompletionStage<? extends U> other,
      BiFunction<? super T, ? super U, ? extends V> fn,
      Executor executor) {
    return super.thenCombineAsync(other, context.wrapFunction(fn), executor);
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBoth(
      CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return super.thenAcceptBoth(other, context.wrapConsumer(action));
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
    return super.thenAcceptBothAsync(other, context.wrapConsumer(action));
  }

  @Override
  public <U> CompletableFuture<Void> thenAcceptBothAsync(
      CompletionStage<? extends U> other,
      BiConsumer<? super T, ? super U> action,
      Executor executor) {
    return super.thenAcceptBothAsync(other, context.wrapConsumer(action), executor);
  }

  @Override
  public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
    return super.runAfterBoth(other, context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
    return super.runAfterBothAsync(other, context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> runAfterBothAsync(
      CompletionStage<?> other, Runnable action, Executor executor) {
    return super.runAfterBothAsync(other, context.wrap(action), executor);
  }

  @Override
  public <U> CompletableFuture<U> applyToEither(
      CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return super.applyToEither(other, context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(
      CompletionStage<? extends T> other, Function<? super T, U> fn) {
    return super.applyToEitherAsync(other, context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> applyToEitherAsync(
      CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
    return super.applyToEitherAsync(other, context.wrapFunction(fn), executor);
  }

  @Override
  public CompletableFuture<Void> acceptEither(
      CompletionStage<? extends T> other, Consumer<? super T> action) {
    return super.acceptEither(other, context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(
      CompletionStage<? extends T> other, Consumer<? super T> action) {
    return super.acceptEitherAsync(other, context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<Void> acceptEitherAsync(
      CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
    return super.acceptEitherAsync(other, context.wrapConsumer(action), executor);
  }

  @Override
  public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
    return super.runAfterEither(other, context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
    return super.runAfterEitherAsync(other, context.wrap(action));
  }

  @Override
  public CompletableFuture<Void> runAfterEitherAsync(
      CompletionStage<?> other, Runnable action, Executor executor) {
    return super.runAfterEitherAsync(other, context.wrap(action), executor);
  }

  @Override
  public <U> CompletableFuture<U> thenCompose(
      Function<? super T, ? extends CompletionStage<U>> fn) {
    return super.thenCompose(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn) {
    return super.thenComposeAsync(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> thenComposeAsync(
      Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
    return super.thenComposeAsync(context.wrapFunction(fn), executor);
  }

  @Override
  public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
    return super.whenComplete(context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
    return super.whenCompleteAsync(context.wrapConsumer(action));
  }

  @Override
  public CompletableFuture<T> whenCompleteAsync(
      BiConsumer<? super T, ? super Throwable> action, Executor executor) {
    return super.whenCompleteAsync(context.wrapConsumer(action), executor);
  }

  @Override
  public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
    return super.handle(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
    return super.handleAsync(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> handleAsync(
      BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
    return super.handleAsync(context.wrapFunction(fn), executor);
  }

  @Override
  public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
    return super.exceptionally(context.wrapFunction(fn));
  }

  @Override
  public <U> CompletableFuture<U> newIncompleteFuture() {
    return new ContextAwareCompletableFuture<>(context);
  }

  @Override
  public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
    return super.completeAsync(context.wrapSupplier(supplier), executor);
  }

  @Override
  public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
    return super.completeAsync(context.wrapSupplier(supplier));
  }
}
