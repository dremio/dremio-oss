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
package com.dremio.dac.service.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.search.SearchDocumentMessageProto;
import com.dremio.service.search.SearchDocumentProto;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/** Test utilities. */
public final class CatalogSearchPublisherTestUtils {
  /** Creates {@link DatasetConfig} with virtual dataset that has given fields. */
  public static DatasetConfig createVirtualDataset(
      NamespaceKey parent, String name, List<ViewFieldType> fields) {
    BatchSchema schema =
        new BatchSchema(
            fields.stream()
                .map(
                    vf ->
                        new Field(
                            vf.getName(), FieldType.nullable(new ArrowType.Int(32, true)), null))
                .collect(Collectors.toUnmodifiableList()));
    return new DatasetConfig()
        .setType(DatasetType.VIRTUAL_DATASET)
        .setVirtualDataset(
            new VirtualDataset().setVersion(DatasetVersion.newVersion()).setSqlFieldsList(fields))
        .setFullPathList(
            ImmutableList.<String>builder().addAll(parent.getPathComponents()).add(name).build())
        .setName(name)
        .setRecordSchema(schema.toByteString());
  }

  /**
   * Verifies that timestamps in {@link SearchDocumentMessageProto.SearchDocumentMessage} are close
   * to current time and clears them.
   */
  public static SearchDocumentMessageProto.SearchDocumentMessage verifyAndClearTimestamps(
      SearchDocumentMessageProto.SearchDocumentMessage message) {
    SearchDocumentMessageProto.SearchDocumentMessage.Builder builder = message.toBuilder();
    SearchDocumentProto.CatalogObject.Builder catalogObjectBuilder =
        builder.getDocumentBuilder().getCatalogObjectBuilder();
    long currentSeconds = Instant.now().toEpochMilli() / 1000;
    assertThat(catalogObjectBuilder.getCreated().getSeconds())
        .isBetween(currentSeconds - 10, currentSeconds);
    assertThat(catalogObjectBuilder.getLastModified().getSeconds())
        .isBetween(currentSeconds - 10, currentSeconds);
    catalogObjectBuilder.clearCreated().clearLastModified();
    return builder.build();
  }
}
