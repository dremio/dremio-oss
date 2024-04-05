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
package com.dremio.telemetry.api.metrics;

import com.dremio.service.BinderImpl;
import com.dremio.service.SingletonRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDynamicContextInserterWithSingletonRegistry {
  @Test
  public void test() {
    SingletonRegistry registry = new SingletonRegistry();

    registry.bind(
        DynamicContextInserter.Mappings.class,
        () -> {
          List<Function<Meter.Id, Tags>> mappings = new ArrayList<>();
          mappings.add(id -> Tags.of("foo", "bar"));
          return mappings;
        });

    registry.bind(
        DynamicContextInserter.class,
        (DynamicContextInserter)
            new BinderImpl.InjectableReference(DynamicContextInserter.class).get(registry));

    DynamicContextInserter dynamicContextInserter = registry.lookup(DynamicContextInserter.class);
    Assertions.assertNotNull(dynamicContextInserter);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    meterRegistry.config().meterFilter(dynamicContextInserter);

    Counter counter = meterRegistry.counter("test");
    List<Tag> tags = counter.getId().getTags();
    Assertions.assertEquals(1, tags.size());
    Assertions.assertEquals("foo", tags.get(0).getKey());
    Assertions.assertEquals("bar", tags.get(0).getValue());
  }
}
