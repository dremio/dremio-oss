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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class DynamicContextInserter implements MeterFilter {
  private final Mappings mappings;

  @Inject
  public DynamicContextInserter(Mappings mappings) {
    this.mappings = mappings;
  }

  @Override
  public Meter.Id map(Meter.Id id) {
    List<Tag> tags =
        mappings.getMappings().stream()
            .flatMap(mapping -> mapping.apply(id).stream())
            .collect(Collectors.toList());
    return id.withTags(tags);
  }

  public interface Mappings {
    List<Function<Meter.Id, Tags>> getMappings();
  }
}
