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
package com.dremio.dac.model.job;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestJobInfoDetailsUI {

  @Test
  public void testGetDatasetName() {
    assertThat(JobInfoDetailsUI.getDatasetName("source.folder1.folder2.file")).isEqualTo("file");
    assertThat(JobInfoDetailsUI.getDatasetName("\"source\".\"folder1\".\"folder2\".\"file\""))
        .isEqualTo("file");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".file"))
        .isEqualTo("file");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".\"file.csv\""))
        .isEqualTo("file.csv");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".\"file.1.csv\""))
        .isEqualTo("file.1.csv");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".file.1.csv"))
        .isEqualTo("csv");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".\"file_csv\""))
        .isEqualTo("file_csv");
    assertThat(JobInfoDetailsUI.getDatasetName("source.\"folder1\".\"folder2\".file_csv"))
        .isEqualTo("file_csv");
  }
}
