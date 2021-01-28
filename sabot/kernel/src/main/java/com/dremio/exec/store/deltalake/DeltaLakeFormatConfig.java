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
package com.dremio.exec.store.deltalake;

import java.util.Objects;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("delta")
public class DeltaLakeFormatConfig implements FormatPluginConfig {
  //format of the data files. Default is parquet.
  private FileType dataFormatType = FileType.PARQUET;

  @Override
  public int hashCode() {
    return Objects.hash(dataFormatType);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    final DeltaLakeFormatConfig config = (DeltaLakeFormatConfig) obj;

    return Objects.equals(dataFormatType, config.dataFormatType);
  }

  @Override
  public String toString() {
    return "DeltaLakeFormatConfig [" +
      dataFormatType.toString() + "]";
  }
}
