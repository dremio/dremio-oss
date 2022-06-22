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
package com.dremio.exec.store.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.dremio.io.file.FileBlockLocation;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Tests for {@link KeyValueSerDe} class
 */
public class KeyValueSerDeTest {
  @Test
  public void testSerDeOfKey() {
    String dataFilePath = "/some/data/file";
    String pluginId = "12345";
    byte[] serializedKey = KeyValueSerDe.serializeKey(dataFilePath, pluginId);
    assertNotNull(serializedKey);

    PartitionProtobuf.DataFileUID deserializedKey = KeyValueSerDe.deserializeKey(serializedKey);
    assertEquals(dataFilePath, deserializedKey.getDataFilePath());
    assertEquals(pluginId, deserializedKey.getPluginId());
  }

  @Test
  public void testSerDeOfValue() {
    List<FileBlockLocation> blockLocations = new ArrayList<>();
    FileBlockLocation mockedBlockLocation = mock(FileBlockLocation.class);
    when(mockedBlockLocation.getOffset()).thenReturn(1L);
    when(mockedBlockLocation.getSize()).thenReturn(128L);
    when(mockedBlockLocation.getHosts()).thenReturn(Collections.singletonList("host:12345"));
    blockLocations.add(mockedBlockLocation);
    byte[] serializedValue = KeyValueSerDe.serializeValue(blockLocations);
    assertNotNull(serializedValue);

    PartitionProtobuf.BlockLocationsList deserializedValue = KeyValueSerDe.deserializeValue(serializedValue);
    assertEquals(1, deserializedValue.getBlockLocationsCount());
    assertEquals(1L, deserializedValue.getBlockLocations(0).getOffset());
    assertEquals(128L, deserializedValue.getBlockLocations(0).getSize());
    assertEquals(1, deserializedValue.getBlockLocations(0).getHostsCount());
    assertEquals("host:12345", deserializedValue.getBlockLocations(0).getHosts(0));
  }
}
