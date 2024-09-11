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
package com.dremio.service.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class TestNamespaceInternalKeyChildKey {
  @Test
  public void testGetChildKey() {
    NamespaceInternalKey key =
        new NamespaceInternalKey(new NamespaceKey(ImmutableList.of("a", "b")));
    assertEquals(
        "2.a.1.b.0.",
        NamespaceInternalKeyDumpUtil.extractRangeKey(
            key.getRangeStartKey().getBytes(StandardCharsets.UTF_8)));
    assertEquals(
        "2.a.1.b.0.c",
        NamespaceInternalKeyDumpUtil.extractKey(
            key.getChildKey("c").getBytes(StandardCharsets.UTF_8), true));
  }
}
