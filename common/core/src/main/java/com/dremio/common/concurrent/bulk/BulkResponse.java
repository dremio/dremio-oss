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
package com.dremio.common.concurrent.bulk;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Representation of a set of responses to a possibly async bulk API call. Each response is uniquely
 * identified by the associated request key, and can be retrieved by that key. Response values are
 * exposed via a CompletionStage which provides an abstraction over the possibly async execution.
 *
 * <p>Bulk API calls may be chained, where a parent bulk API call needs to delegate some work to a
 * child API call. These parent -> child call chains may or may not share common request key or
 * response value types. A single parent call may also depend on multiple child calls. To support
 * these scenarios, BulkResponse exposes transform operations for both response keys and values, as
 * well as a combineWith operation for combining two responses into one.
 *
 * @param <KEY> Type of the keys in the response object.
 * @param <VAL> Type of the values in the response object.
 */
public final class BulkResponse<KEY, VAL> {

  private final Map<KEY, Response<KEY, VAL>> responses;

  private BulkResponse(Map<KEY, Response<KEY, VAL>> responses) {
    this.responses = responses;
  }

  /**
   * Returns the Response object for a request key, or null if there is no response for that key.
   *
   * @param key The request key to get the response for.
   */
  public Response<KEY, VAL> get(KEY key) {
    return responses.get(key);
  }

  /** Returns a set of all keys contained in this BulkResponse. */
  public Set<KEY> keys() {
    return Collections.unmodifiableSet(responses.keySet());
  }

  /** Returns a collection of all Response objects contained in this BulkResponse. */
  public Collection<Response<KEY, VAL>> responses() {
    return Collections.unmodifiableCollection(responses.values());
  }

  public void forEach(Consumer<? super Response<KEY, VAL>> action) {
    responses.values().forEach(action);
  }

  /**
   * Transforms a BulkResponse by applying a key transformation to each individual response. The
   * response values are unchanged.
   *
   * @param keyTransformer The key transformation function. Note that the direction of the key
   *     transformation is from child response to parent response, which is the reverse of key
   *     transformations done during request transforms.
   * @return The transformed BulkResponse object.
   * @param <KEY2> Type of the transformed keys.
   */
  public <KEY2> BulkResponse<KEY2, VAL> transform(Function<KEY, KEY2> keyTransformer) {
    return new BulkResponse<>(
        responses.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    entry -> keyTransformer.apply(entry.getKey()),
                    entry -> entry.getValue().transform(keyTransformer))));
  }

  /**
   * Transforms a BulkResponse by applying both a key transformation and a value transformation to
   * each individual response.
   *
   * @param keyTransformer The key transformation function. Note that the direction of the key
   *     transformation is from child response to parent response, which is the reverse of key
   *     transformations done during request transforms.
   * @param valueTransformer The value transformation function, converting from a child response
   *     value to a parent response value.
   * @return The transformed BulkResponse object.
   * @param <KEY2> Type of the transformed keys.
   * @param <VAL2> Type of the transformed values.
   */
  public <KEY2, VAL2> BulkResponse<KEY2, VAL2> transform(
      Function<KEY, KEY2> keyTransformer, ValueTransformer<KEY, VAL, KEY2, VAL2> valueTransformer) {
    return new BulkResponse<>(
        responses.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    entry -> keyTransformer.apply(entry.getKey()),
                    entry -> entry.getValue().transform(keyTransformer, valueTransformer))));
  }

  /**
   * Transforms a BulkResponse by applying both a key transformation and a value transformation to
   * each individual response. The value transformation execution is possibly async.
   *
   * @param keyTransformer The key transformation function. Note that the direction of the key
   *     transformation is from child response to parent response, which is the reverse of key
   *     transformations done during request transforms.
   * @param valueTransformer The value transformation function, converting from a child response
   *     value to a parent response value.
   * @return The transformed BulkResponse object.
   * @param <KEY2> Type of the transformed keys.
   * @param <VAL2> Type of the transformed values.
   */
  public <KEY2, VAL2> BulkResponse<KEY2, VAL2> transformAsync(
      Function<KEY, KEY2> keyTransformer,
      ValueTransformer<KEY, VAL, KEY2, CompletionStage<VAL2>> valueTransformer) {
    return new BulkResponse<>(
        responses.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    entry -> keyTransformer.apply(entry.getKey()),
                    entry -> entry.getValue().transformAsync(keyTransformer, valueTransformer))));
  }

  /**
   * Combines this BulkResponse with another BulkResponse object. Response values for the same
   * request key are combined using the provided combiner function. If a response value for a
   * request key is only present in one of the BulkResponse objects, that value will be used and the
   * combiner function will not be called.
   *
   * @param other BulkResponse to combine with.
   * @param valueCombiner The value combiner function. This will only be called if non-null values
   *     are present in both responses for a given request key.
   * @return A BulkResponse object with the combined response values.
   */
  public BulkResponse<KEY, VAL> combineWith(
      BulkResponse<KEY, VAL> other, BiFunction<VAL, VAL, VAL> valueCombiner) {
    Set<KEY> allKeys = Sets.union(responses.keySet(), other.responses.keySet());
    return new BulkResponse<>(
        allKeys.stream()
            .map(
                key ->
                    Response.combine(responses.get(key), other.responses.get(key), valueCombiner))
            .filter(Objects::nonNull)
            .collect(ImmutableMap.toImmutableMap(Response::key, Function.identity())));
  }

  /**
   * Returns an empty BulkResponse object.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static <KEY, VAL> BulkResponse<KEY, VAL> empty() {
    return new BulkResponse<>(ImmutableMap.of());
  }

  /**
   * Creates a new Builder object that can be used to construct a BulkResponse.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static <KEY, VAL> Builder<KEY, VAL> builder() {
    return new Builder<>();
  }

  /**
   * Creates a new Builder object that can be used to construct a BulkResponse.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static <KEY, VAL> Builder<KEY, VAL> builder(int expectedSize) {
    return new Builder<>(expectedSize);
  }

  /**
   * Creates a new Collector object that can be used with the Java stream API.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static <KEY, VAL>
      Collector<BulkResponse<KEY, VAL>, Builder<KEY, VAL>, BulkResponse<KEY, VAL>> collector() {
    return new ResponseCollector<>();
  }

  /**
   * Represents a response associated with a single request key. The response value is exposed via a
   * CompletionStage which may wrap an async computation. Each Response object maintains a
   * cumulative elapsed time that includes time spent computing/transforming its response value, as
   * well as any time spent in child responses
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static final class Response<KEY, VAL> {
    private final MutableLong elapsedNanos;
    private final KEY key;
    private final CompletionStage<VAL> response;

    /**
     * Constructs a Response from a constant value.
     *
     * @param key The request key for the response.
     * @param precomputedValue The constant response value.
     */
    public Response(KEY key, VAL precomputedValue) {
      this.elapsedNanos = new MutableLong(0);
      this.key = key;
      this.response = CompletableFuture.completedFuture(precomputedValue);
    }

    /**
     * Constructs a response from an existing CompletionStage.
     *
     * @param key The request key for the response.
     * @param response The CompletionStage to associate with the response.
     */
    public Response(KEY key, CompletionStage<VAL> response) {
      this.elapsedNanos = new MutableLong(0);
      this.key = key;
      this.response = timedFuture(response);
    }

    /**
     * Constructs a response from an existing CompletionStage.
     *
     * @param key The request key for the response.
     * @param response The CompletionStage to associate with the response.
     * @param elapsedNanos Previous computation time to associate with this response.
     */
    public Response(KEY key, CompletionStage<VAL> response, MutableLong elapsedNanos) {
      this.elapsedNanos = elapsedNanos;
      this.key = key;
      this.response = response;
    }

    /**
     * Constructs a new Response from this Response by applying a key transformation.
     *
     * @param keyTransformer The key transformation function
     * @param <KEY2> New key type
     */
    public <KEY2> Response<KEY2, VAL> transform(Function<KEY, KEY2> keyTransformer) {
      KEY2 transformedKey = keyTransformer.apply(key);
      return new Response<>(transformedKey, response, elapsedNanos);
    }

    /**
     * Constructs a new Response from this Response by applying a key transformation and a value
     * transformation.
     *
     * @param keyTransformer The key transformation function
     * @param valueTransformer The value transformation function
     * @param <KEY2> New key type
     * @param <VAL2> New value type
     */
    public <KEY2, VAL2> Response<KEY2, VAL2> transform(
        Function<KEY, KEY2> keyTransformer,
        ValueTransformer<KEY, VAL, KEY2, VAL2> valueTransformer) {
      KEY2 transformedKey = keyTransformer.apply(key);
      CompletionStage<VAL2> transformedValue =
          response.thenApply(v -> timedCall(() -> valueTransformer.apply(key, transformedKey, v)));
      return new Response<>(transformedKey, transformedValue, elapsedNanos);
    }

    /**
     * Constructs a new Response from this Response by applying a key transformation and a value
     * transformation. The value transformation execution is possibly async.
     *
     * @param keyTransformer The key transformation function
     * @param valueTransformer The value transformation function
     * @param <KEY2> New key type
     * @param <VAL2> New value type
     */
    public <KEY2, VAL2> Response<KEY2, VAL2> transformAsync(
        Function<KEY, KEY2> keyTransformer,
        ValueTransformer<KEY, VAL, KEY2, CompletionStage<VAL2>> valueTransformer) {
      KEY2 transformedKey = keyTransformer.apply(key);
      CompletionStage<VAL2> transformedValue =
          response.thenCompose(
              v -> timedCall(() -> timedFuture(valueTransformer.apply(key, transformedKey, v))));
      return new Response<>(transformedKey, transformedValue, elapsedNanos);
    }

    /** Returns the key for this response. */
    public KEY key() {
      return key;
    }

    /** Returns the CompletionStage for this response's value. */
    public CompletionStage<VAL> response() {
      return response;
    }

    /**
     * Returns the elapsed time for this response.
     *
     * @param timeUnit The desired TimeUnit for the returned time.
     */
    public long elapsed(TimeUnit timeUnit) {
      return timeUnit.convert(elapsedNanos.longValue(), TimeUnit.NANOSECONDS);
    }

    /**
     * Combines two responses in an async-safe fashion using a combiner function. If both responses
     * are non-null, CompletionStage.thenCombine is called using the provided function to perform
     * the combination. If either response is null, the non-null response is returned as the
     * combined response.
     *
     * @param first The first Response object.
     * @param second The second Response object.
     * @param valueCombiner The value combination function.
     * @return The combined Response object, or null if both first and second are null.
     * @param <KEY> Type of the keys in the response object.
     * @param <VAL> Type of the values in the response object.
     */
    private static <KEY, VAL> Response<KEY, VAL> combine(
        Response<KEY, VAL> first,
        Response<KEY, VAL> second,
        BiFunction<VAL, VAL, VAL> valueCombiner) {
      Response<KEY, VAL> combinedResponse;
      if (first != null) {
        if (second != null) {
          Preconditions.checkArgument(
              first.key.equals(second.key), "Responses with different keys cannot be combined");
          MutableLong elapsed = new MutableLong(0);
          CompletionStage<VAL> combinedStage =
              first.response.thenCombine(
                  second.response,
                  (f, s) -> {
                    elapsed.add(
                        Math.max(first.elapsedNanos.longValue(), second.elapsedNanos.longValue()));
                    return valueCombiner.apply(f, s);
                  });
          combinedResponse = new Response<>(first.key, combinedStage, elapsed);
        } else {
          combinedResponse = first;
        }
      } else {
        combinedResponse = second;
      }

      return combinedResponse;
    }

    private <T> T timedCall(Callable<T> callable) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      try {
        return callable.call();
      } catch (Exception ex) {
        Throwables.throwIfUnchecked(ex);
        throw new RuntimeException(ex);
      } finally {
        elapsedNanos.addAndGet(stopwatch.elapsed().toNanos());
      }
    }

    private <T> CompletionStage<T> timedFuture(CompletionStage<T> f) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      return f.whenComplete((k, v) -> elapsedNanos.addAndGet(stopwatch.elapsed().toNanos()));
    }
  }

  /**
   * A Builder implementation that can be used to construct BulkResponse objects. This can be used
   * for rare cases where manual construction is necessary. In general, prefer using the BulkRequest
   * functions which return a BulkResponse to construct responses instead.
   *
   * <p>Attempting to add duplicate keys will result in a failure.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  public static final class Builder<KEY, VAL> {
    private final ImmutableMap.Builder<KEY, Response<KEY, VAL>> mapBuilder;

    Builder() {
      this.mapBuilder = ImmutableMap.builder();
    }

    Builder(int expectedSize) {
      this.mapBuilder = ImmutableMap.builderWithExpectedSize(expectedSize);
    }

    public BulkResponse<KEY, VAL> build() {
      return new BulkResponse<>(mapBuilder.build());
    }

    public Builder<KEY, VAL> add(Response<KEY, VAL> response) {
      mapBuilder.put(response.key(), response);
      return this;
    }

    public Builder<KEY, VAL> add(KEY key, VAL precomputedValue) {
      mapBuilder.put(key, new Response<>(key, precomputedValue));
      return this;
    }

    public Builder<KEY, VAL> add(KEY key, CompletionStage<VAL> response) {
      mapBuilder.put(key, new Response<>(key, response));
      return this;
    }

    public Builder<KEY, VAL> add(KEY key, CompletionStage<VAL> response, MutableLong elapsedNanos) {
      mapBuilder.put(key, new Response<>(key, response, elapsedNanos));
      return this;
    }

    public Builder<KEY, VAL> add(KEY key, CompletionStage<VAL> response, long elapsedNanos) {
      mapBuilder.put(key, new Response<>(key, response, new MutableLong(elapsedNanos)));
      return this;
    }

    public Builder<KEY, VAL> addAll(BulkResponse<KEY, VAL> bulkResponse) {
      mapBuilder.putAll(bulkResponse.responses);
      return this;
    }

    public Builder<KEY, VAL> addAll(Builder<KEY, VAL> responseBuilder) {
      mapBuilder.putAll(responseBuilder.mapBuilder.build());
      return this;
    }
  }

  /**
   * A Collector implementation which uses a Builder to combine multiple BulkResponse objects using
   * the Java stream API. This does not support combining responses with duplicate response keys.
   * Use BulkResponse.combineWith in cases where responses being combined may share keys.
   *
   * @param <KEY> Type of the keys in the response object.
   * @param <VAL> Type of the values in the response object.
   */
  private static final class ResponseCollector<KEY, VAL>
      implements Collector<BulkResponse<KEY, VAL>, Builder<KEY, VAL>, BulkResponse<KEY, VAL>> {

    private static final Set<Characteristics> CHARACTERISTICS =
        ImmutableSet.of(Characteristics.UNORDERED);

    @Override
    public Supplier<Builder<KEY, VAL>> supplier() {
      return Builder::new;
    }

    @Override
    public BiConsumer<Builder<KEY, VAL>, BulkResponse<KEY, VAL>> accumulator() {
      return Builder::addAll;
    }

    @Override
    public BinaryOperator<Builder<KEY, VAL>> combiner() {
      return Builder::addAll;
    }

    @Override
    public Function<Builder<KEY, VAL>, BulkResponse<KEY, VAL>> finisher() {
      return Builder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return CHARACTERISTICS;
    }
  }
}
