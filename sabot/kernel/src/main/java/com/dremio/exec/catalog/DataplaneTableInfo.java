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
package com.dremio.exec.catalog;

import com.dremio.plugins.ExternalNamespaceEntry;

public class DataplaneTableInfo {
  private final String name;
  private final String uniqueInstanceId;
  private final String path;
  private final long createdAt;
  private final String tableId;
  private final ExternalNamespaceEntry.Type formatType;
  private final String sourceId;
  private final String schema;

  public DataplaneTableInfo(newBuilder builder) {
    this.name = builder.name;
    this.uniqueInstanceId = builder.uniqueInstanceId;
    this.path = builder.path;
    this.createdAt = builder.createdAt;
    this.tableId = builder.tableId;
    this.formatType = builder.formatType;
    this.sourceId = builder.sourceId;
    this.schema = builder.schema;
  }

  public static class newBuilder {

    private String name;
    private String uniqueInstanceId;
    private String path;
    private long createdAt;
    private String tableId;
    private ExternalNamespaceEntry.Type formatType;
    private String sourceId;
    private String schema;
    public newBuilder() {
    }

    public newBuilder name(String name) {
      this.name = name;
      return this;
    }

    public newBuilder tag(String tag) {
      this.uniqueInstanceId = tag;
      return this;
    }

    public newBuilder path(String path) {
      this.path = path;
      return this;
    }

    public newBuilder createdAt(long createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public newBuilder tableId(String tableId) {
      this.tableId = tableId;
      return this;
    }

    public newBuilder formatType(ExternalNamespaceEntry.Type formatType) {
      this.formatType = formatType;
      return this;
    }

    public newBuilder sourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public newBuilder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public DataplaneTableInfo build() {
      DataplaneTableInfo dataplaneTableInfo =  new DataplaneTableInfo(this);
      validateDataplaneTableInfo(dataplaneTableInfo);
      return dataplaneTableInfo;
    }
    private void validateDataplaneTableInfo(DataplaneTableInfo dataplaneTableInfo) {
      //Todo: Validate all columns that are not nullable
      if (dataplaneTableInfo.tableId == null) {
        throw new IllegalStateException();
      }

      if (dataplaneTableInfo.sourceId == null) {
        throw new IllegalStateException();
      }
    }
  }

  public String getName() {
    return this.name;
  }

  public String getUniqueInstanceId() {
    return this.uniqueInstanceId;
  }

  public String getPath() {
    return this.path;
  }

  public long getCreatedAt() {
    return this.createdAt;
  }

  public String getTableId() {return this.tableId;}

  public ExternalNamespaceEntry.Type getFormatType() {return this.formatType;}

  public String getSourceId() {return this.sourceId;}

  public String getSchema() {return this.schema;}
}
