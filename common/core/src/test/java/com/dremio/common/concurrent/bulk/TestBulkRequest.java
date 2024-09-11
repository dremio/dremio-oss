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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestBulkRequest extends BulkTestBase {

  @Test
  public void testBuildAndIterateKeys() {
    BulkRequest.Builder<String> builder = BulkRequest.builder();
    BulkRequest<String> request = builder.add("a").add("b").add("c").build();

    assertThat(request.requests()).containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  public void testPartition() {
    BulkRequest<String> request =
        new BulkRequest<>(ImmutableSet.of("a", "abc", "ac", "b", "bac", "bc", "c"));
    Map<Integer, BulkRequest<String>> byLength = request.partition(String::length);

    Assertions.assertThat(byLength).containsOnlyKeys(1, 2, 3);
    assertThat(byLength.get(1).requests()).containsExactlyInAnyOrder("a", "b", "c");
    assertThat(byLength.get(2).requests()).containsExactlyInAnyOrder("ac", "bc");
    assertThat(byLength.get(3).requests()).containsExactlyInAnyOrder("abc", "bac");
  }

  @Test
  public void testHandleRequestsWithConstantResponse() throws Exception {
    BulkRequest<String> request = new BulkRequest<>(ImmutableSet.of("a", "b", "c"));
    BulkResponse<String, String> response = request.handleRequests("x");

    validateResponses(response, ImmutableMap.of("a", "x", "b", "x", "c", "x"), ImmutableMap.of());
  }

  @Test
  public void testHandleRequestsWithAsyncValue() throws Exception {
    BulkRequest<String> request = new BulkRequest<>(ImmutableSet.of("a", "b", "c"));
    // returns either "key=x", or throws an exception if the key is "c-xform"
    BulkResponse<String, String> response =
        request.handleRequests(
            v ->
                v.equals("c")
                    ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                    : CompletableFuture.completedFuture(v + "=x"));

    validateResponses(
        response,
        ImmutableMap.of("a", "a=x", "b", "b=x"),
        ImmutableMap.of("c", new RuntimeException("failed")));
  }

  @Test
  public void testBulkTransformAndHandleRequestsKeysOnly() throws Exception {
    BulkRequest<String> request = new BulkRequest<>(ImmutableSet.of("a", "b", "c"));
    // bulk function which returns either "key=x", or throws an exception if the key is "c-xform"
    BulkFunction<String, String> bulkFunction =
        req ->
            req.handleRequests(
                v ->
                    v.equals("c-xform")
                        ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                        : CompletableFuture.completedFuture(v + "=x"));

    // transform key from "val" to "val-xform"
    Function<String, String> keyTransformer = k -> k + "-xform";

    BulkResponse<String, String> response =
        request.bulkTransformAndHandleRequests(bulkFunction, keyTransformer);

    validateResponses(
        response,
        ImmutableMap.of("a", "a-xform=x", "b", "b-xform=x"),
        ImmutableMap.of("c", new RuntimeException("failed")));
  }

  @Test
  public void testBulkTransformAndHandleRequestsKeysAndValues() throws Exception {
    BulkRequest<String> request = new BulkRequest<>(ImmutableSet.of("a", "b", "c"));
    // bulk function which returns either the value "x", or throws an exception if the key
    // is "c-xform"
    BulkFunction<String, String> bulkFunction =
        req ->
            req.handleRequests(
                v ->
                    v.equals("c-xform")
                        ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                        : CompletableFuture.completedFuture("x"));

    // transform key from "val" to "val-xform"
    Function<String, String> keyTransformer = k -> k + "-xform";

    // transform returned value from "val" to "transformedkey=val"
    ValueTransformer<String, String, String, String> valueTransformer =
        (transformedKey, originalKey, value) -> transformedKey + "=x";

    BulkResponse<String, String> response =
        request.bulkTransformAndHandleRequests(bulkFunction, keyTransformer, valueTransformer);

    validateResponses(
        response,
        ImmutableMap.of("a", "a-xform=x", "b", "b-xform=x"),
        ImmutableMap.of("c", new RuntimeException("failed")));
  }

  @Test
  public void testBulkTransformValuesAsync() throws Exception {
    BulkRequest<String> request = new BulkRequest<>(ImmutableSet.of("a", "b", "c", "d"));
    // bulk function which returns either the value "x", or throws an exception if the key is "c"
    BulkFunction<String, String> bulkFunction =
        req ->
            req.handleRequests(
                v ->
                    v.equals("c")
                        ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                        : CompletableFuture.completedFuture("x"));

    // transform returned value from "val" to "key=val", or throws an exception if the key is "d"
    ValueTransformer<String, String, String, CompletionStage<String>> asyncValueTransformer =
        (originalKey, transformedKey, value) ->
            transformedKey.equals("d")
                ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                : CompletableFuture.completedFuture(transformedKey + "=x");

    BulkResponse<String, String> response =
        request.bulkTransformAndHandleRequestsAsync(
            bulkFunction, Function.identity(), asyncValueTransformer);

    validateResponses(
        response,
        ImmutableMap.of("a", "a=x", "b", "b=x"),
        ImmutableMap.of("c", new RuntimeException("failed"), "d", new RuntimeException("failed")));
  }

  @Test
  public void testBulkPartitionAndHandleRequests() throws Exception {
    BulkRequest<String> request =
        new BulkRequest<>(ImmutableSet.of("a", "abc", "ac", "b", "bac", "bc", "c"));

    // partition on string length
    Function<String, Integer> partitioner = String::length;

    // partitioned bulk function which returns either the value "x;part=#", or throws an exception
    // if the key is "c-xform"
    Function<Integer, BulkFunction<String, String>> partitionBulkFunctions =
        p ->
            req ->
                req.handleRequests(
                    v ->
                        v.equals("c-xform")
                            ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                            : CompletableFuture.completedFuture("x;part=" + p.toString()));

    // transform key from "val" to "val-xform"
    Function<String, String> keyTransformer = k -> k + "-xform";

    // transform returned value from "val" to "transformedkey=val"
    ValueTransformer<String, String, String, String> valueTransformer =
        (transformedKey, originalKey, value) -> transformedKey + "=" + value;

    BulkResponse<String, String> response =
        request.bulkPartitionAndHandleRequests(
            partitioner, partitionBulkFunctions, keyTransformer, valueTransformer);

    validateResponses(
        response,
        ImmutableMap.of(
            "a",
            "a-xform=x;part=1",
            "abc",
            "abc-xform=x;part=3",
            "ac",
            "ac-xform=x;part=2",
            "b",
            "b-xform=x;part=1",
            "bac",
            "bac-xform=x;part=3",
            "bc",
            "bc-xform=x;part=2"),
        ImmutableMap.of("c", new RuntimeException("failed")));
  }
}
