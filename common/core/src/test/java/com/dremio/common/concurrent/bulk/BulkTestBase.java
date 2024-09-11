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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BulkTestBase {
  <K, V> void validateResponses(
      BulkResponse<K, V> responses, Map<K, V> expectedValues, Map<K, Throwable> expectedErrors)
      throws Exception {
    List<K> actualKeys =
        responses.responses().stream().map(BulkResponse.Response::key).collect(Collectors.toList());
    assertThat(actualKeys)
        .containsExactlyInAnyOrderElementsOf(
            Sets.union(expectedValues.keySet(), expectedErrors.keySet()));

    for (K key : actualKeys) {
      if (expectedErrors.containsKey(key)) {
        Throwable error = expectedErrors.get(key);
        assertThatThrownBy(() -> responses.get(key).response().toCompletableFuture().get())
            .as("response '%s'", key)
            .hasRootCauseMessage(error.getMessage())
            .hasRootCauseInstanceOf(error.getClass());
      } else if (expectedValues.containsKey(key)) {
        assertThat(responses.get(key).response().toCompletableFuture().get())
            .as("response '%s'", key)
            .isEqualTo(expectedValues.get(key));
      }
    }
  }
}
