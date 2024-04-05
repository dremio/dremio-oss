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
package com.dremio.exec.physical.config.copyinto;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TestCopyIntoQueryProperties {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPropertiesAccessors() {
    CopyIntoQueryProperties properties = new CopyIntoQueryProperties();
    properties.setOnErrorOption(CopyIntoQueryProperties.OnErrorOption.CONTINUE);
    properties.setStorageLocation("/path/to/storage");

    assertThat(properties.getOnErrorOption())
        .isEqualTo(CopyIntoQueryProperties.OnErrorOption.CONTINUE);
    assertThat(properties.getStorageLocation()).isEqualTo("/path/to/storage");
  }

  @Test
  public void testPropertiesConstructor() {
    CopyIntoQueryProperties properties =
        new CopyIntoQueryProperties(CopyIntoQueryProperties.OnErrorOption.ABORT, "/data");

    assertThat(properties.getOnErrorOption())
        .isEqualTo(CopyIntoQueryProperties.OnErrorOption.ABORT);
    assertThat(properties.getStorageLocation()).isEqualTo("/data");
  }

  @Test
  public void testJsonSerDe() throws JsonProcessingException {
    CopyIntoQueryProperties propOrg =
        new CopyIntoQueryProperties(CopyIntoQueryProperties.OnErrorOption.CONTINUE, "/data");
    assertThat(propOrg.getRecordStateEvents())
        .containsExactlyInAnyOrder(
            CopyIntoFileLoadInfo.CopyIntoFileState.PARTIALLY_LOADED,
            CopyIntoFileLoadInfo.CopyIntoFileState.SKIPPED);
    propOrg.addEventHistoryRecordsForState(CopyIntoFileLoadInfo.CopyIntoFileState.FULLY_LOADED);

    String propJson = OBJECT_MAPPER.writeValueAsString(propOrg);

    CopyIntoQueryProperties propDeserialized =
        OBJECT_MAPPER.readValue(propJson, CopyIntoQueryProperties.class);
    assertThat(propOrg.getRecordStateEvents()).isEqualTo(propDeserialized.getRecordStateEvents());
    assertThat(propOrg.getOnErrorOption()).isEqualTo(propDeserialized.getOnErrorOption());
    assertThat(propOrg.getStorageLocation()).isEqualTo(propDeserialized.getStorageLocation());
  }
}
