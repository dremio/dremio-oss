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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;

/**
 * Test conversions during upgrade
 */
public class TestNamespaceUpgrade {
  private NameSpaceContainerSerializer serializer = new NameSpaceContainerSerializer();

  private void checkContainer(NameSpaceContainer orig, NameSpaceContainer converted, UpdateMode expectedUpdateMode) {
    if (expectedUpdateMode == null) {
      assertEquals(orig, converted);
    } else {
      assertNotNull(orig.getSource());
      assertNotNull(orig.getSource().getMetadataPolicy());
      assertEquals(expectedUpdateMode, converted.getSource().getMetadataPolicy().getDatasetUpdateMode());
      NameSpaceContainer origCopy = serializer.revert(serializer.convert(orig));
      origCopy.getSource().getMetadataPolicy().setDatasetUpdateMode(expectedUpdateMode);
      assertEquals(origCopy, converted);
    }
  }

  /**
   * Check if 'sourceConfig' conversion through both json and byte[] converts its update mode correctly
   * @param expectedUpdateMode  if null, expect 'sourceConfig' to convert identically.
   *                            If not null, expect the only difference to be the update mode
   */
  private void checkConversion(UpdateMode expectedUpdateMode, SourceConfig sourceConfig) throws Exception {
    NameSpaceContainer orig = new NameSpaceContainer()
        .setType(NameSpaceContainer.Type.SOURCE)
        .setSource(sourceConfig);

    NameSpaceContainer jsonConverted = serializer.fromJson(serializer.toJson(orig));
    checkContainer(orig, jsonConverted, expectedUpdateMode);
    NameSpaceContainer byteAryConverted = serializer.revert(serializer.convert(orig));
    checkContainer(orig, byteAryConverted, expectedUpdateMode);
  }

  @Test
  public void testUpdateModeInline() throws Exception {
    checkConversion(null, new SourceConfig()
        .setName("a"));

    checkConversion(null, new SourceConfig()
        .setName("a")
        .setMetadataPolicy(new MetadataPolicy()));

    checkConversion(null, new SourceConfig()
        .setName("a")
        .setMetadataPolicy(new MetadataPolicy()
            .setDatasetUpdateMode(UpdateMode.PREFETCH)));

    checkConversion(null, new SourceConfig()
        .setName("a")
        .setMetadataPolicy(new MetadataPolicy()
            .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)));

    checkConversion(UpdateMode.PREFETCH_QUERIED, new SourceConfig()
        .setName("a")
        .setMetadataPolicy(new MetadataPolicy()
            .setDatasetUpdateMode(UpdateMode.INLINE)));
  }
}
