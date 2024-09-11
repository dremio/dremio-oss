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
package com.dremio.datastore;

import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_KEY;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_VALUE_NONE;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_KEY;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_VALUE_ERROR;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_VALUE_SUCCESS;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTimedKVStore {
  private TimedKVStore<String, String> timedKVStore;
  private KVStore<String, String> kvStore;
  private MeterRegistry simpleRegistry;

  @Before
  public void init() throws Exception {
    simpleRegistry = new SimpleMeterRegistry();
    io.micrometer.core.instrument.Metrics.addRegistry(simpleRegistry);
    kvStore = Mockito.mock(NoopKVStore.class);
    timedKVStore = new TimedKVStore<>(kvStore);
  }

  @After
  public void tearDown() throws Exception {
    timedKVStore = null;
    kvStore = null;
  }

  @Test
  public void testMetrics() {
    Mockito.when(kvStore.getName()).thenReturn("NoopStore");
    Mockito.when(kvStore.get("foo")).thenThrow(new DatastoreException("not found"));
    try {
      timedKVStore.get("foo");
    } catch (final DatastoreException e) {
    }

    Document<String, String> doc =
        new Document<String, String>() {
          @Override
          public String getKey() {
            return "foo";
          }

          @Override
          public String getValue() {
            return "bar";
          }

          @Override
          public String getTag() {
            return null;
          }
        };
    Mockito.when(kvStore.put("foo", "bar")).thenReturn(doc);
    timedKVStore.put("foo", "bar");

    Mockito.reset(kvStore);
    Mockito.when(kvStore.get("foo")).thenReturn(doc);
    Mockito.when(kvStore.getName()).thenReturn("NoopStore");
    timedKVStore.get("foo");

    final Tags successfulTags =
        Tags.of(
            "name",
            kvStore.getName(),
            TAG_OUTCOME_KEY,
            TAG_OUTCOME_VALUE_SUCCESS,
            TAG_EXCEPTION_KEY,
            TAG_EXCEPTION_VALUE_NONE);

    Assert.assertEquals(
        "get count should be 1",
        1,
        Timer.builder("kvstore.operations")
            .withRegistry(simpleRegistry)
            .withTags(successfulTags.and("op", "get"))
            .count());
    Assert.assertEquals(
        String.format("[reg: %s] put count should be 1", simpleRegistry.getClass().getSimpleName()),
        1,
        Timer.builder("kvstore.operations")
            .withRegistry(simpleRegistry)
            .withTags(successfulTags.and("op", "put"))
            .count());
    Assert.assertEquals(
        "get count with exceptions should be 1",
        1,
        Timer.builder("kvstore.operations")
            .withRegistry(simpleRegistry)
            .withTags(
                successfulTags
                    .and("op", "get")
                    .and(TAG_EXCEPTION_KEY, "DatastoreException")
                    .and(TAG_OUTCOME_KEY, TAG_OUTCOME_VALUE_ERROR))
            .count());
  }
}
