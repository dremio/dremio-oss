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

public final class DataplaneViewInfo {
  private final String viewId;
  private final String spaceId;
  private final String viewName;
  private final String schemaId;
  private final String path;
  private final String tag;
  private final long createdAt;
  private final String sqlDefinition;
  private final String sqlContext;

  private DataplaneViewInfo(newBuilder builder) {
    this.viewId = builder.viewId;
    this.spaceId = builder.spaceId;
    this.viewName = builder.viewName;
    this.schemaId = builder.schemaId;
    this.path = builder.path;
    this.tag = builder.tag;
    this.createdAt = builder.createdAt;
    this.sqlDefinition = builder.sqlDefinition;
    this.sqlContext = builder.sqlContext;
  }

  public static class newBuilder {
    private String viewId;
    private String spaceId;
    private String viewName;
    private String schemaId;
    private String path;
    private String tag;
    private long createdAt;
    private String sqlDefinition;
    private String sqlContext;

    public newBuilder() {
    }

    public newBuilder viewId(String viewId) {
      this.viewId = viewId;
      return this;
    }

    public newBuilder spaceId(String spaceId) {
      this.spaceId = spaceId;
      return this;
    }
    public newBuilder viewName(String viewName) {
      this.viewName = viewName;
      return this;
    }
    public newBuilder schemaId (String schemaId) {
      this.schemaId = schemaId;
      return this;
    }

    public newBuilder path (String path) {
      this.path = path;
      return this;
    }

    public newBuilder tag (String tag) {
      this.tag = tag;
      return this;
    }

    public newBuilder createdAt(long createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public newBuilder sqlDefinition(String sqlDefinition) {
      this.sqlDefinition = sqlDefinition;
      return this;
    }

    public newBuilder sqlContext(String sqlContext) {
      this.sqlContext = sqlContext;
      return this;
    }
    public DataplaneViewInfo build() {
      DataplaneViewInfo dataplaneViewInfo =  new DataplaneViewInfo(this);
      validateDataplaneViewInfo(dataplaneViewInfo);
      return dataplaneViewInfo;
    }
    private void validateDataplaneViewInfo(DataplaneViewInfo dataplaneViewInfo) {
      if (dataplaneViewInfo.viewId == null) {
        throw new IllegalStateException();
      }

      if (dataplaneViewInfo.spaceId == null) {
        throw new IllegalStateException();
      }

      if (dataplaneViewInfo.sqlDefinition == null) {
        throw new IllegalStateException();
      }
    }
  }

  public String getViewId() {
    return this.viewId;
  }

  public String getSpaceId() {
    return this.spaceId;
  }

  public String getViewName() {
    return this.viewName;
  }

  public String getSchemaId() {
    return this.schemaId;
  }

  public String getPath() {return this.path;}

  public String getTag() {return this.tag;}

  public long getCreatedAt() {return this.createdAt;}

  public String getSqlDefinition() {return this.sqlDefinition;}

  public String getSqlContext() {return this.sqlContext;}
}
