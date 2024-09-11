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

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.Test;

public class TestBulkResponse extends BulkTestBase {

  @Test
  public void testTransformKeysOnly() throws Exception {
    BulkResponse<String, String> response =
        BulkResponse.<String, String>builder()
            .add("a", "a=x")
            .add("b", "b=y")
            .add("c", CompletableFuture.failedFuture(new RuntimeException("failed")))
            .build();

    // transform key from "val" to "val-xform"
    Function<String, String> keyTransformer = k -> k + "-xform";

    BulkResponse<String, String> transformed = response.transform(keyTransformer);

    validateResponses(
        transformed,
        ImmutableMap.of("a-xform", "a=x", "b-xform", "b=y"),
        ImmutableMap.of("c-xform", new RuntimeException("failed")));
  }

  @Test
  public void testTransformKeysAndValues() throws Exception {
    BulkResponse<String, String> response =
        BulkResponse.<String, String>builder()
            .add("a", "x")
            .add("b", "y")
            .add("c", CompletableFuture.failedFuture(new RuntimeException("failed")))
            .build();

    // transform key from "val" to "val-xform"
    Function<String, String> keyTransformer = k -> k + "-xform";

    // transform returned value from "val" to "originalkey=val"
    ValueTransformer<String, String, String, String> valueTransformer =
        (originalKey, transformedKey, value) -> originalKey + "=" + value;

    BulkResponse<String, String> transformed = response.transform(keyTransformer, valueTransformer);

    validateResponses(
        transformed,
        ImmutableMap.of("a-xform", "a=x", "b-xform", "b=y"),
        ImmutableMap.of("c-xform", new RuntimeException("failed")));
  }

  @Test
  public void testTransformValueAsync() throws Exception {
    BulkResponse<String, String> response =
        BulkResponse.<String, String>builder()
            .add("a", "x")
            .add("b", "y")
            .add("c", "z")
            .add("d", CompletableFuture.failedFuture(new RuntimeException("failed")))
            .build();

    // transform returned value from "val" to "key=val", or throws exception if key is "c"
    ValueTransformer<String, String, String, CompletionStage<String>> asyncValueTransformer =
        (originalKey, transformedKey, value) ->
            originalKey.equals("c")
                ? CompletableFuture.failedFuture(new RuntimeException("failed"))
                : CompletableFuture.completedFuture(originalKey + "=" + value);

    BulkResponse<String, String> transformed =
        response.transformAsync(Function.identity(), asyncValueTransformer);

    validateResponses(
        transformed,
        ImmutableMap.of("a", "a=x", "b", "b=y"),
        ImmutableMap.of("c", new RuntimeException("failed"), "d", new RuntimeException("failed")));
  }

  @Test
  public void testCombineWith() throws Exception {
    BulkResponse<String, String> response1 =
        BulkResponse.<String, String>builder()
            .add("a", "x1")
            .add("b", "y1")
            .add("c", CompletableFuture.failedFuture(new RuntimeException("failed1")))
            .build();

    BulkResponse<String, String> response2 =
        BulkResponse.<String, String>builder().add("b", "y2").add("c", "z2").add("d", "w2").build();

    BulkResponse<String, String> combined =
        response1.combineWith(response2, (v1, v2) -> v1 + ":" + v2);

    validateResponses(
        combined,
        ImmutableMap.of("a", "x1", "b", "y1:y2", "d", "w2"),
        ImmutableMap.of("c", new RuntimeException("failed1")));
  }
}
