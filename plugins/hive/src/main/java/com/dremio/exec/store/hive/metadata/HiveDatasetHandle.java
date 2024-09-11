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
package com.dremio.exec.store.hive.metadata;

import java.util.Objects;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.options.InternalMetadataTableOption;
import org.apache.hadoop.hive.metastore.api.Table;

public final class HiveDatasetHandle implements DatasetHandle {

  private final EntityPath datasetpath;
  private final InternalMetadataTableOption internalMetadataTableOption;
  private final Table table;

  private HiveDatasetHandle(final EntityPath datasetpath,
                            final InternalMetadataTableOption internalMetadataTableOption,
                            final Table table) {
    this.datasetpath = datasetpath;
    this.internalMetadataTableOption = internalMetadataTableOption;
    this.table = table;
  }

  @Override
  public EntityPath getDatasetPath() {
    return datasetpath;
  }

  public InternalMetadataTableOption getInternalMetadataTableOption() {
    return internalMetadataTableOption;
  }

  public Table getTable() {
    return table;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private EntityPath datasetpath;
    private InternalMetadataTableOption internalMetadataTableOption;
    private Table table;

    public Builder datasetpath(EntityPath datasetpath) {
      this.datasetpath = datasetpath;
      return this;
    }

    public Builder internalMetadataTableOption(InternalMetadataTableOption internalMetadataTableOption) {
      this.internalMetadataTableOption = internalMetadataTableOption;
      return this;
    }

    public Builder table(Table table) {
      this.table = table;
      return this;
    }

    public HiveDatasetHandle build() {
      Objects.requireNonNull(datasetpath, "dataset path is required");
      return new HiveDatasetHandle(datasetpath, internalMetadataTableOption, table);
    }
  }
}
