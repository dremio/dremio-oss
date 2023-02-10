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
package com.dremio.exec.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for {@link com.dremio.exec.util.LongRange}
 */
public class TestLongRange {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSerDe() throws JsonProcessingException {
    LongRange raw = new LongRange(10L, 12L);
    String json = OBJECT_MAPPER.writeValueAsString(raw);
    assertThat(json).isEqualTo("{\"min\":10,\"max\":12}", json);

    LongRange deserialized = OBJECT_MAPPER.readValue(json, LongRange.class);
    assertThat(raw).isEqualTo(deserialized);
  }

  @Test
  public void testIsNotInRange() {
    assertThat((new LongRange(1L, 3L)).isNotInRange(4L)).isTrue();
    assertThat((new LongRange(1L, 3L)).isNotInRange(0L)).isTrue();

    assertThat((new LongRange(1L, 3L)).isNotInRange(2L)).isFalse();

    // Test boundaries
    assertThat((new LongRange(1L, 3L)).isNotInRange(1L)).isFalse();
    assertThat((new LongRange(1L, 3L)).isNotInRange(3L)).isFalse();
  }
}
