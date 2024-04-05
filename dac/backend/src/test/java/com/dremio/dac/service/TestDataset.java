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
package com.dremio.dac.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.dac.api.Dataset;
import com.dremio.service.namespace.file.proto.ParquetFileConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class TestDataset {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSerializeDataset() throws Exception {
    String json = OBJECT_MAPPER.writeValueAsString(createDataset());
    assertThat(json).contains("\"fields\"");
    assertThat(json).contains("\"isMetadataExpired\":null");
    assertThat(json).contains("\"lastMetadataRefreshAt\":null");
  }

  @Test
  public void testSerializeDataset_withValidity() throws Exception {
    Dataset dataset = createDataset();
    dataset.setMetadataExpired(false, 1706911655_000L);
    String json = OBJECT_MAPPER.writeValueAsString(dataset);
    assertThat(json).contains("\"isMetadataExpired\":false");
    assertThat(json).contains("\"lastMetadataRefreshAt\":\"2024-02-02T22:07:35.000Z\"");
  }

  @Test
  public void testDeserializeDataset_fieldsAreNull() throws Exception {
    Dataset dataset = createDataset();
    String json = OBJECT_MAPPER.writeValueAsString(dataset);
    Dataset newDataset =
        OBJECT_MAPPER.readValue(json.getBytes(StandardCharsets.UTF_8), Dataset.class);
    assertThat(dataset.getFields()).isNotNull();
    assertThat(newDataset.getFields()).isNull();
  }

  private Dataset createDataset() {
    return new Dataset(
        "id",
        Dataset.DatasetType.PHYSICAL_DATASET,
        ImmutableList.of("@user", "table"),
        ImmutableList.of(new Field("field", FieldType.notNullable(ArrowType.Bool.INSTANCE), null)),
        null,
        null,
        null,
        null,
        null,
        new ParquetFileConfig(),
        null);
  }
}
