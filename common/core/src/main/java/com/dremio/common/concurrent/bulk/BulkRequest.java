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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Representation of a set of requests to a possibly async bulk API call. Each individual request is
 * uniquely identified by its request key.
 *
 * <p>Bulk API calls may be chained, where a parent bulk API call needs to delegate some work to a
 * child API call. These parent -> child call chains may or may not share common request key or
 * response value types. A single parent call may also depend on multiple child calls. To support
 * these scenarios, BulkRequest exposes transform and partition operations for response keys.
 *
 * <p>BulkRequest also exposes methods for handling requests in various fashions to produce a
 * BulkResponse. These range from simple use cases with constant response values, up to more complex
 * use cases where request handling is done by calling one or more child BulkFunctions which may
 * require partitioning and/or key & value transformations.
 *
 * @param <KEY> Type of the request keys.
 */
public final class BulkRequest<KEY> {

  /**
   * Map of keys for this request. Maps to a possibly precomputed elapsed time associated with the
   * request key.
   */
  private final Map<KEY, Long> requests;

  BulkRequest(Set<KEY> requests) {
    this.requests = requests.stream().collect(Collectors.toMap(Function.identity(), r -> 0L));
  }

  BulkRequest(Map<KEY, Long> requests) {
    this.requests = requests;
  }

  public Set<KEY> requests() {
    return Collections.unmodifiableSet(requests.keySet());
  }

  public int size() {
    return requests.size();
  }

  public void forEach(Consumer<? super KEY> action) {
    requests.keySet().forEach(action);
  }

  /**
   * Partitions a BulkRequest into multiple sub-requests using the specified partitioning function,
   * which maps key values to partition values. Partition functions may return a null partition
   * value.
   *
   * @param partitioner The partitioning function.
   * @return A map of BulkRequest objects keyed by partition.
   * @param <PAR> Type of the partition values.
   */
  public <PAR> Map<PAR, BulkRequest<KEY>> partition(Function<KEY, PAR> partitioner) {
    Map<PAR, Map<KEY, Long>> partitionedRequests =
        requests.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> partitioner.apply(e.getKey()),
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    return partitionedRequests.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> new BulkRequest<>(entry.getValue())));
  }

  /**
   * Handle this request by creating a BulkResponse object with a constant response value for each
   * request key.
   *
   * @param constantResponseValue The constant response value.
   * @return The created BulkResponse object.
   * @param <VAL> Type of the response value.
   */
  public <VAL> BulkResponse<KEY, VAL> handleRequests(VAL constantResponseValue) {
    BulkResponse.Builder<KEY, VAL> responseBuilder = BulkResponse.builder(requests.size());
    requests.forEach(
        (key, elapsed) ->
            responseBuilder.add(
                key, CompletableFuture.completedFuture(constantResponseValue), elapsed));
    return responseBuilder.build();
  }

  /**
   * Handle this request by creating a BulkResponse object having response values computed using the
   * provided function. Response values are exposed via a CompletionStage, allowing for async
   * computation where desired.
   *
   * @param asyncValueSupplier The async value supplier function.
   * @return The created BulkResponse object.
   * @param <VAL> Type of the response value.
   */
  public <VAL> BulkResponse<KEY, VAL> handleRequests(
      Function<KEY, CompletionStage<VAL>> asyncValueSupplier) {
    BulkResponse.Builder<KEY, VAL> responseBuilder = BulkResponse.builder(requests.size());
    requests.forEach(
        (key, elapsed) -> {
          MutableLong elapsedNanos = new MutableLong(elapsed);
          Stopwatch stopwatch = Stopwatch.createStarted();
          try {
            CompletionStage<VAL> asyncVal = asyncValueSupplier.apply(key);
            elapsedNanos.add(stopwatch.elapsed().toNanos());
            asyncVal = timedFuture(asyncVal, elapsedNanos);
            responseBuilder.add(key, asyncVal, elapsedNanos);
          } catch (Exception ex) {
            elapsedNanos.add(stopwatch.elapsed().toNanos());
            CompletionStage<VAL> failed = CompletableFuture.failedFuture(ex);
            responseBuilder.add(key, failed, elapsedNanos);
          }
        });
    return responseBuilder.build();
  }

  /**
   * Handle this request by first transforming the request keys using the provided key
   * transformation function, then passing the transformed BulkRequest to a BulkFunction to produce
   * a BulkResponse in the transformed key space. This BulkResponse is then transformed back using a
   * reverse key mapping to produce a BulkResponse in the original request key space.
   *
   * @param bulkFunction The bulk function to call with the transformed request.
   * @param keyTransformer The key transformation function. This must be a one-to-one (bijective)
   *     function.
   * @return The BulkResponse in the original (caller's) key space.
   * @param <VAL> Type of the response values.
   * @param <KEY2> Type of the transformed keys accepted by the provided BulkFunction.
   */
  public <VAL, KEY2> BulkResponse<KEY, VAL> bulkTransformAndHandleRequests(
      BulkFunction<KEY2, VAL> bulkFunction, Function<KEY, KEY2> keyTransformer) {

    // transform request keys, and keep a reverse mapping from transformed keys to original keys
    Map<KEY2, KEY> reverseKeyLookup = new HashMap<>();
    // The key transformation could be non-trivial, so we record and add to final response below
    Map<KEY, Long> keyTransformationTime = new HashMap<>();
    BulkRequest.Builder<KEY2> requestBuilder = BulkRequest.builder(size());
    BulkResponse.Builder<KEY, VAL> responseBuilder = BulkResponse.builder(size());
    transformKeys(
        requests,
        keyTransformer,
        reverseKeyLookup,
        keyTransformationTime,
        requestBuilder,
        responseBuilder);

    // call the provided bulk function with the transformed request
    BulkRequest<KEY2> transformedRequest = requestBuilder.build();
    BulkResponse<KEY2, VAL> responses = bulkFunction.apply(transformedRequest);
    responses.forEach(
        response -> {
          KEY originalKey = reverseKeyLookup.get(response.key());
          MutableLong elapsedNanos = new MutableLong(keyTransformationTime.get(originalKey));
          CompletionStage<VAL> asyncVal = addElapsedTime(response, elapsedNanos);
          responseBuilder.add(originalKey, asyncVal, elapsedNanos);
        });
    return responseBuilder.build();
  }

  /**
   * Handle this request by first transforming the request keys using the provided key
   * transformation function, then passing the transformed BulkRequest to a BulkFunction to produce
   * a BulkResponse in the transformed key space. This BulkResponse is then transformed back using
   * both a reverse key mapping and a value transformation to produce a BulkResponse in the original
   * key space.
   *
   * @param bulkFunction The bulk function to call with the transformed request.
   * @param keyTransformer The key transformation function. This must be a one-to-one (bijective)
   *     function.
   * @param valueTransformer The value transformation function. This transforms response values from
   *     the value returned by the provided BulkFunction to the value to be returned by this call.
   * @return The BulkResponse in the original (caller's) key space.
   * @param <VAL> Type of the transformed response values.
   * @param <KEY2> Type of the transformed keys accepted by the provided BulkFunction.
   * @param <VAL2> Type of the response values returned by the provided BulkFunction.
   */
  public <VAL, KEY2, VAL2> BulkResponse<KEY, VAL> bulkTransformAndHandleRequests(
      BulkFunction<KEY2, VAL2> bulkFunction,
      Function<KEY, KEY2> keyTransformer,
      ValueTransformer<KEY2, VAL2, KEY, VAL> valueTransformer) {

    // transform request keys, and keep a reverse mapping from transformed keys to original keys
    Map<KEY2, KEY> reverseKeyLookup = new HashMap<>();
    // The key transformation could be non-trivial, so we record and add to final response below
    Map<KEY, Long> keyTransformationTime = new HashMap<>();
    BulkRequest.Builder<KEY2> requestBuilder = BulkRequest.builder(size());
    BulkResponse.Builder<KEY, VAL> responseBuilder = BulkResponse.builder(size());
    transformKeys(
        requests,
        keyTransformer,
        reverseKeyLookup,
        keyTransformationTime,
        requestBuilder,
        responseBuilder);

    // call the provided bulk function with the transformed request
    BulkRequest<KEY2> transformedRequest = requestBuilder.build();
    BulkResponse<KEY2, VAL2> responses = bulkFunction.apply(transformedRequest);
    responses.forEach(
        response -> {
          KEY originalKey = reverseKeyLookup.get(response.key());
          MutableLong elapsedNanos = new MutableLong(keyTransformationTime.get(originalKey));
          BulkResponse.Response<KEY, VAL> transformedResponse =
              response.transform(reverseKeyLookup::get, valueTransformer);
          CompletionStage<VAL> asyncVal = addElapsedTime(transformedResponse, elapsedNanos);
          responseBuilder.add(originalKey, asyncVal, elapsedNanos);
        });
    return responseBuilder.build();
  }

  public <VAL, KEY2, VAL2> BulkResponse<KEY, VAL> bulkTransformAndHandleRequestsAsync(
      BulkFunction<KEY2, VAL2> bulkFunction,
      Function<KEY, KEY2> keyTransformer,
      ValueTransformer<KEY2, VAL2, KEY, CompletionStage<VAL>> valueTransformer) {

    // transform request keys, and keep a reverse mapping from transformed keys to original keys
    Map<KEY2, KEY> reverseKeyLookup = new HashMap<>();
    // The key transformation could be non-trivial, so we record and add to final response below
    Map<KEY, Long> keyTransformationTime = new HashMap<>();
    BulkRequest.Builder<KEY2> requestBuilder = BulkRequest.builder(size());
    BulkResponse.Builder<KEY, VAL> responseBuilder = BulkResponse.builder(size());
    transformKeys(
        requests,
        keyTransformer,
        reverseKeyLookup,
        keyTransformationTime,
        requestBuilder,
        responseBuilder);

    // call the provided bulk function with the transformed request
    BulkRequest<KEY2> transformedRequest = requestBuilder.build();
    BulkResponse<KEY2, VAL2> responses = bulkFunction.apply(transformedRequest);
    responses.forEach(
        response -> {
          KEY originalKey = reverseKeyLookup.get(response.key());
          MutableLong elapsedNanos = new MutableLong(keyTransformationTime.get(originalKey));
          BulkResponse.Response<KEY, VAL> transformedResponse =
              response.transformAsync(reverseKeyLookup::get, valueTransformer);
          CompletionStage<VAL> asyncVal = addElapsedTime(transformedResponse, elapsedNanos);
          responseBuilder.add(originalKey, asyncVal, elapsedNanos);
        });
    return responseBuilder.build();
  }

  /**
   * Handle a request by partitioning it into multiple sub-requests, then passing each sub-request
   * to a BulkFunction selected based on the partition. First, the request is partitioned using the
   * provided partitioning function. For each partition, the BulkFunction to call is determined by
   * calling the partitionBulkFunction function, which maps from a partition value to a
   * BulkFunction. Finally, bulkTransformAndHandleRequests is called on each partition's
   * sub-request, using the selected BulkFunction, key transformation and value transformation
   * functions. Responses for each partition are then combined into a single BulkResponse.
   *
   * @param partitioner The partitioning function.
   * @param partitionBulkFunction A function which takes a partition value as input and returns the
   *     BulkFunction to use for that partition.
   * @param keyTransformer The key transformation function. This must be a one-to-one (bijective)
   *     function.
   * @param valueTransformer The value transformation function. This transforms response values from
   *     the value returned by the partition's BulkFunction to the value to be returned by this
   *     call.
   * @return The combined BulkResponse in the original (caller's) key space.
   * @param <PAR> Type of the partition values.
   * @param <VAL> Type of the transformed response values.
   * @param <KEY2> Type of the transformed keys accepted by the provided BulkFunction.
   * @param <VAL2> Type of the response values returned by the provided BulkFunction.
   */
  public <PAR, VAL, KEY2, VAL2> BulkResponse<KEY, VAL> bulkPartitionAndHandleRequests(
      Function<KEY, PAR> partitioner,
      Function<PAR, BulkFunction<KEY2, VAL2>> partitionBulkFunction,
      Function<KEY, KEY2> keyTransformer,
      ValueTransformer<KEY2, VAL2, KEY, VAL> valueTransformer) {
    Map<PAR, Map<KEY, Long>> partitionedRequests =
        requests.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> partitioner.apply(e.getKey()),
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    return partitionedRequests.entrySet().stream()
        .map(
            entry -> {
              PAR partition = entry.getKey();
              BulkRequest<KEY> partitionRequest = new BulkRequest<>(entry.getValue());
              return partitionRequest.bulkTransformAndHandleRequests(
                  partitionBulkFunction.apply(partition), keyTransformer, valueTransformer);
            })
        .collect(BulkResponse.collector());
  }

  /**
   * Creates a new Builder object that can be used to construct a BulkRequest.
   *
   * @param <KEY> Type of the request keys.
   */
  public static <KEY> Builder<KEY> builder() {
    return new Builder<>();
  }

  /**
   * Creates a new Builder object that can be used to construct a BulkRequest.
   *
   * @param <KEY> Type of the request keys.
   */
  public static <KEY> Builder<KEY> builder(int expectedSize) {
    return new Builder<>(expectedSize);
  }

  private static <KEY, KEY2, VAL> void transformKeys(
      Map<KEY, Long> keys,
      Function<KEY, KEY2> keyTransformer,
      Map<KEY2, KEY> reverseKeyLookup,
      Map<KEY, Long> keyTransformationTime,
      BulkRequest.Builder<KEY2> requestBuilder,
      BulkResponse.Builder<KEY, VAL> responseBuilder) {
    keys.forEach(
        (key, elapsed) -> {
          Stopwatch stopwatch = Stopwatch.createStarted();
          long elapsedNanos = elapsed;
          try {
            KEY2 transformedKey = keyTransformer.apply(key);
            elapsedNanos += stopwatch.elapsed().toNanos();
            keyTransformationTime.put(key, elapsedNanos);
            reverseKeyLookup.put(transformedKey, key);
            requestBuilder.add(transformedKey);
          } catch (Exception ex) {
            elapsedNanos += stopwatch.elapsed().toNanos();
            CompletionStage<VAL> failed = CompletableFuture.failedFuture(ex);
            responseBuilder.add(key, failed, elapsedNanos);
          }
        });
  }

  private static <T> CompletionStage<T> timedFuture(
      CompletionStage<T> f, MutableLong elapsedNanos) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    return f.whenComplete((r, ex) -> elapsedNanos.add(stopwatch.elapsed().toNanos()));
  }

  private static <K, V> CompletionStage<V> addElapsedTime(
      BulkResponse.Response<K, V> response, MutableLong elapsedNanos) {
    return response
        .response()
        .whenComplete((r, ex) -> elapsedNanos.add(response.elapsed(TimeUnit.NANOSECONDS)));
  }

  /**
   * A Builder implementation that can be used to construct BulkRequest objects.
   *
   * @param <KEY> Type of the request keys.
   */
  public static final class Builder<KEY> {
    private final ImmutableMap.Builder<KEY, Long> setBuilder;

    Builder() {
      this.setBuilder = ImmutableMap.builder();
    }

    Builder(int expectedSize) {
      this.setBuilder = ImmutableMap.builderWithExpectedSize(expectedSize);
    }

    public BulkRequest<KEY> build() {
      return new BulkRequest<>(setBuilder.buildKeepingLast());
    }

    public Builder<KEY> add(KEY request) {
      setBuilder.put(request, 0L);
      return this;
    }

    public Builder<KEY> add(KEY request, long elapsed) {
      setBuilder.put(request, elapsed);
      return this;
    }

    public Builder<KEY> addAll(Iterable<KEY> requests) {
      requests.forEach(key -> setBuilder.put(key, 0L));
      return this;
    }
  }
}
