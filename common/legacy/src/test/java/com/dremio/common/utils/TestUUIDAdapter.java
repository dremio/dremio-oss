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
package com.dremio.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.UUID;

import org.junit.Test;

/**
 * Unit tests for UUIDAdapter
 */
public class TestUUIDAdapter {

  @Test
  public void testGet16BytesFromUUID() {
    UUID uuid = UUID.randomUUID();
    byte[] result = UUIDAdapter.getBytesFromUUID(uuid);
    assertEquals("Expected result to be a byte array with 16 elements.", 16, result.length);
  }

  @Test
  public void testGenerateSameUUIDFromBytes() {
    UUID uuid = UUID.randomUUID();
    byte[] bytes = UUIDAdapter.getBytesFromUUID(uuid);
    UUID reconstructedUUID = UUIDAdapter.getUUIDFromBytes(bytes);
    assertEquals(uuid, reconstructedUUID);
  }

  @Test
  public void testShouldNotGenerateSameUUIDFromBytes() {
    UUID uuid = UUID.fromString("38400000-8cf0-11bd-b23e-10b96e4ef00d");
    byte[] result = UUIDAdapter.getBytesFromUUID(uuid);
    UUID newUuid = UUID.nameUUIDFromBytes(result);
    assertFalse(uuid.equals(newUuid));
  }
}
