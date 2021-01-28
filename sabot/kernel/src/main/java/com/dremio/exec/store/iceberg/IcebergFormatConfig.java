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
package com.dremio.exec.store.iceberg;

import java.util.List;
import java.util.Objects;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.file.proto.IcebergMetaStoreType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

@JsonTypeName("iceberg") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class IcebergFormatConfig implements FormatPluginConfig {
  public String outputExtension = "avro";
  private static final List<String> DEFAULT_EXTS = ImmutableList.of("avro");

  private IcebergMetaStoreType metaStoreType = IcebergMetaStoreType.HDFS;
  private FileType dataFormatType = FileType.PARQUET;
  private FormatPluginConfig dataFormatConfig = new ParquetFormatConfig();

  public FileType getDataFormatType() {
    return dataFormatType;
  }

  public void setDataFormatType(FileType dataFormatType) {
    this.dataFormatType = dataFormatType;
  }

  public FormatPluginConfig getDataFormatConfig() {
    return dataFormatConfig;
  }

  public void setDataFormatConfig(FormatPluginConfig dataFormatConfig) {
    this.dataFormatConfig = dataFormatConfig;
  }

  public IcebergMetaStoreType getMetaStoreType() {
    return metaStoreType;
  }

  public void setMetaStoreType(IcebergMetaStoreType metaStoreType) {
    this.metaStoreType = metaStoreType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metaStoreType, dataFormatType, dataFormatConfig);
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

    final IcebergFormatConfig other = (IcebergFormatConfig) obj;

    return Objects.equals(metaStoreType, other.metaStoreType)
      && Objects.equals(dataFormatType, other.dataFormatType)
      && dataFormatConfig.equals(other.dataFormatConfig);
  }

  @Override
  public String toString() {
    return "IcebergFormatConfig [" +
      " metaStoreType " + metaStoreType +
      "(" + dataFormatConfig.toString() + ")" +
      "]";
  }

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    return DEFAULT_EXTS;
  }
}
