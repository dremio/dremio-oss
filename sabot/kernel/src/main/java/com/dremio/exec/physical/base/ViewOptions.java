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
package com.dremio.exec.physical.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

/**
 * Save properties needed for creating views
 */
public class ViewOptions {
  private final ResolvedVersionContext version;
  private final BatchSchema batchSchema; // tracks the columns of the table to create from
  private final ActionType actionType;
  private final Map<String, String> properties;

  private ViewOptions(ViewOptionsBuilder builder) {
    this.version = builder.version;
    this.batchSchema = builder.batchSchema;
    this.actionType = builder.actionType;
    this.properties = builder.properties;
  }

  public enum ActionType {
    CREATE_VIEW,
    UPDATE_VIEW,
    ALTER_VIEW
  }

  public ResolvedVersionContext getVersion(){
    return version;
  }

  public BatchSchema getBatchSchema() { return batchSchema; }

  public ActionType getActionType() { return actionType; }

  public boolean isViewCreate() { return actionType == ActionType.CREATE_VIEW; }

  public boolean isViewUpdate() { return actionType == ActionType.UPDATE_VIEW; }

  public boolean isViewAlter() { return actionType == ActionType.ALTER_VIEW; }

  public Map<String, String> getProperties() { return properties; }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ViewOptions that = (ViewOptions) o;
    return Objects.equals(version, that.version)
      && Objects.equals(batchSchema, that.batchSchema)
      && actionType == that.actionType
      && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, batchSchema, actionType, properties);
  }

  public static class ViewOptionsBuilder {
    private ResolvedVersionContext version;
    private BatchSchema batchSchema;
    private ActionType actionType;
    private Map<String, String> properties;

    public ViewOptionsBuilder() {
    }

    public ViewOptionsBuilder version(ResolvedVersionContext version){
      Preconditions.checkNotNull(version);
      this.version = version;
      return this;
    }

    public ViewOptionsBuilder batchSchema(BatchSchema schema){
      Preconditions.checkNotNull(schema);
      this.batchSchema = schema;
      return this;
    }

    public ViewOptionsBuilder actionType(ActionType actionType) {
      this.actionType = actionType;
      return this;
    }

    public ViewOptionsBuilder properties(Map<String, String> properties) {
      Preconditions.checkArgument(!properties.isEmpty());
      this.properties = new HashMap<>(properties);
      return this;
    }

    public ViewOptions build() {
      ViewOptions viewOptions = new ViewOptions(this);
      return viewOptions;
    }
  }
}
