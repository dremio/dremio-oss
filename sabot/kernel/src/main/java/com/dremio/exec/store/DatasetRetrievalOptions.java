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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.dremio.connector.metadata.ExtendedPropertyOption;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import com.dremio.connector.metadata.options.DirListInputSplitType;
import com.dremio.connector.metadata.options.IgnoreAuthzErrors;
import com.dremio.connector.metadata.options.MaxLeafFieldCount;
import com.dremio.connector.metadata.options.MaxNestedFieldLevels;
import com.dremio.exec.catalog.AllowAutoPromote;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.base.Preconditions;

import io.protostuff.ByteString;

/**
 * Dataset retrieval options.
 */
public class DatasetRetrievalOptions {

  private static final Optional<Boolean> DEFAULT_AUTO_PROMOTE_OPTIONAL =
      Optional.ofNullable(System.getProperty("dremio.datasets.auto_promote"))
          .map(Boolean::parseBoolean);

  static final boolean DEFAULT_AUTO_PROMOTE;
  public static final DatasetRetrievalOptions DEFAULT;
  public static final DatasetRetrievalOptions IGNORE_AUTHZ_ERRORS;
  public static final int DEFAULT_MAX_METADATA_LEAF_COLUMNS = 800;
  public static final int DEFAULT_MAX_NESTED_LEVEL = 16;

  static {
    DEFAULT_AUTO_PROMOTE = DEFAULT_AUTO_PROMOTE_OPTIONAL.orElse(false);

    DEFAULT = newBuilder()
        .setIgnoreAuthzErrors(false)
        .setDeleteUnavailableDatasets(true)
        .setAutoPromote(DEFAULT_AUTO_PROMOTE)
        .setForceUpdate(false)
        .setRefreshDataset(false)
        .setMaxMetadataLeafColumns(DEFAULT_MAX_METADATA_LEAF_COLUMNS)
        .setMaxNestedLevel(DEFAULT_MAX_NESTED_LEVEL)
        .build();

    IGNORE_AUTHZ_ERRORS =
        DEFAULT.toBuilder()
            .setIgnoreAuthzErrors(true)
            .build();
  }

  private final Optional<Boolean> ignoreAuthzErrors;
  private final Optional<Boolean> deleteUnavailableDatasets;
  private final Optional<Boolean> autoPromote;
  private final Optional<Boolean> forceUpdate;
  private final Optional<Boolean> refreshDataset;
  private final Optional<Integer> maxMetadataLeafColumns;
  private final Optional<Integer> maxNestedLevel;

  private DatasetRetrievalOptions fallback;

  /**
   * Use {@link #newBuilder() builder}.
   */
  DatasetRetrievalOptions(Builder builder) {
    this.ignoreAuthzErrors = Optional.ofNullable(builder.ignoreAuthzErrors);
    this.deleteUnavailableDatasets = Optional.ofNullable(builder.deleteUnavailableDatasets);
    this.autoPromote = Optional.ofNullable(builder.autoPromote);
    this.forceUpdate = Optional.ofNullable(builder.forceUpdate);
    this.refreshDataset = Optional.ofNullable(builder.refreshDataset);
    this.maxMetadataLeafColumns = Optional.ofNullable(builder.maxMetadataLeafColumns);
    this.maxNestedLevel = Optional.ofNullable(builder.maxNestedLevel);
  }


  public boolean ignoreAuthzErrors() {
    return ignoreAuthzErrors.orElseGet(() -> fallback.ignoreAuthzErrors());
  }

  public boolean deleteUnavailableDatasets() {
    return deleteUnavailableDatasets.orElseGet(() -> fallback.deleteUnavailableDatasets());
  }

  public boolean autoPromote() {
    return autoPromote.orElseGet(() -> fallback.autoPromote());
  }

  public boolean forceUpdate() {
    return forceUpdate.orElseGet(() -> fallback.forceUpdate());
  }

  public boolean refreshDataset() {
    return refreshDataset.orElseGet(() -> fallback.refreshDataset());
  }

  public int maxMetadataLeafColumns() {
    return maxMetadataLeafColumns.orElseGet(() -> fallback.maxMetadataLeafColumns());
  }

  public int maxNestedLevel() {
    return maxNestedLevel.orElseGet(() -> fallback.maxNestedLevel());
  }

  public DatasetRetrievalOptions withFallback(DatasetRetrievalOptions fallback) {
    this.fallback = fallback;
    return this;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setIgnoreAuthzErrors(ignoreAuthzErrors.orElse(null))
        .setDeleteUnavailableDatasets(deleteUnavailableDatasets.orElse(null))
        .setAutoPromote(autoPromote.orElse(null))
        .setForceUpdate(forceUpdate.orElse(null))
        .setRefreshDataset(refreshDataset.orElse(null))
        .setMaxMetadataLeafColumns(maxMetadataLeafColumns.orElse(DEFAULT_MAX_METADATA_LEAF_COLUMNS))
        .setMaxNestedLevel(maxNestedLevel.orElse(DEFAULT_MAX_NESTED_LEVEL));
  }

  public static class Builder {

    private Boolean ignoreAuthzErrors;
    private Boolean deleteUnavailableDatasets;
    private Boolean autoPromote;
    private Boolean forceUpdate;
    private boolean refreshDataset;
    private Integer maxMetadataLeafColumns;
    private Integer maxNestedLevel;
    private List<String> filesList = new ArrayList<>();
    private Map<String, String> partition = new LinkedHashMap<>();

    private Builder() {
    }

    public Builder setIgnoreAuthzErrors(Boolean ignoreAuthzErrors) {
      this.ignoreAuthzErrors = ignoreAuthzErrors;
      return this;
    }

    public Builder setDeleteUnavailableDatasets(Boolean deleteUnavailableDatasets) {
      this.deleteUnavailableDatasets = deleteUnavailableDatasets;
      return this;
    }

    public Builder setAutoPromote(Boolean autoPromote) {
      this.autoPromote = autoPromote;
      return this;
    }

    public Builder setForceUpdate(Boolean forceUpdate) {
      this.forceUpdate = forceUpdate;
      return this;
    }

    public Builder setRefreshDataset(Boolean refreshDataset) {
      this.refreshDataset = refreshDataset;
      return this;
    }

    public Builder setMaxMetadataLeafColumns(Integer maxMetadataLeafColumns) {
      this.maxMetadataLeafColumns = maxMetadataLeafColumns;
      return this;
    }

    public Builder setMaxNestedLevel(Integer maxNestedLevel) {
      this.maxNestedLevel = maxNestedLevel;
      return this;
    }

    public Builder setFilesList(List<String> filesList) {
      this.filesList.addAll(filesList);
      return this;
    }

    public Builder setPartition(Map<String, String> partition) {
      this.partition.putAll(partition);
      return this;
    }

    public DatasetRetrievalOptions build() {
      if (!filesList.isEmpty()) {
        return new DatasetRetrievalFilesListOptions(this, filesList);
      } else if (!partition.isEmpty()) {
        return new DatasetRetrievalPartitionOptions(this, partition);
      }

      return new DatasetRetrievalOptions(this);
    }
  }

  /**
   * Creates a {@link Builder}.
   *
   * @return new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static DatasetRetrievalOptions of(MetadataOption[] options) {

    Builder b = new Builder();

    for(MetadataOption o : options) {
      if(o instanceof IgnoreAuthzErrors) {
        b = b.setIgnoreAuthzErrors(true);
      }else if(o instanceof AllowAutoPromote) {
        b.setAutoPromote(true);
      } else if(o instanceof MaxLeafFieldCount) {
        b.setMaxMetadataLeafColumns(((MaxLeafFieldCount) o).getValue());
      } else if(o instanceof MaxNestedFieldLevels) {
        b.setMaxNestedLevel(((MaxNestedFieldLevels) o).getValue());
      }
    }

    //TODO: better defaults?
    return b.build().withFallback(DEFAULT);
  }

  /**
   * Convert a {@link MetadataPolicy} to {@link DatasetRetrievalOptions}.
   *
   * @param policy metadata policy
   * @return dataset retrieval options
   */
  public static DatasetRetrievalOptions fromMetadataPolicy(MetadataPolicy policy) {
    Preconditions.checkNotNull(policy.getAutoPromoteDatasets());
    Preconditions.checkNotNull(policy.getDeleteUnavailableDatasets());

    return newBuilder()
        .setAutoPromote(policy.getAutoPromoteDatasets())
        .setDeleteUnavailableDatasets(policy.getDeleteUnavailableDatasets())
        // not an option in policy (or UI)
        .setForceUpdate(false)
        .setRefreshDataset(false)
        // not an option in policy (or UI)
        .setIgnoreAuthzErrors(false)
        .setMaxMetadataLeafColumns(DEFAULT_MAX_METADATA_LEAF_COLUMNS)
        .setMaxNestedLevel(DEFAULT_MAX_NESTED_LEVEL)
        .build();
  }

  public GetDatasetOption[] asGetDatasetOptions(DatasetConfig datasetConfig) {
    List<GetDatasetOption> options = new ArrayList<>();
    if (autoPromote()) {
      options.add(new AllowAutoPromote());
    }

    if (ignoreAuthzErrors()) {
      options.add(new IgnoreAuthzErrors());
    }

    options.add(new MaxLeafFieldCount(maxMetadataLeafColumns()));
    options.add(new MaxNestedFieldLevels(maxNestedLevel()));

    addDatasetOptions(GetDatasetOption.class, datasetConfig, options);

    return options.toArray(new GetDatasetOption[options.size()]);
  }

  public GetMetadataOption[] asGetMetadataOptions(DatasetConfig datasetConfig) {
    List<GetMetadataOption> options = new ArrayList<>();
    options.add(new MaxLeafFieldCount(maxMetadataLeafColumns()));
    options.add(new MaxNestedFieldLevels(maxNestedLevel()));

    addDatasetOptions(GetMetadataOption.class, datasetConfig, options);
    return options.toArray(new GetMetadataOption[options.size()]);
  }

  public ListPartitionChunkOption[] asListPartitionChunkOptions(DatasetConfig datasetConfig) {
    List<ListPartitionChunkOption> options = new ArrayList<>();
    options.add(new MaxLeafFieldCount(maxMetadataLeafColumns()));
    options.add(new MaxNestedFieldLevels(maxNestedLevel()));

    if (refreshDataset()) {
      options.add(new DirListInputSplitType());
    }

    addCustomOptions(options);

    addDatasetOptions(ListPartitionChunkOption.class, datasetConfig, options);
    return options.toArray(new ListPartitionChunkOption[options.size()]);
  }

  protected void addCustomOptions(List<ListPartitionChunkOption> options) {}

  private <T extends MetadataOption> void addDatasetOptions(Class<T> clazz, DatasetConfig datasetConfig, List<T> outOptions) {
    if(datasetConfig == null) {
      return;
    }

    List<MetadataOption> options = new ArrayList<>();
    if(datasetConfig.getPhysicalDataset() != null && datasetConfig.getPhysicalDataset().getFormatSettings() != null) {
      options.add(new FileConfigOption(datasetConfig.getPhysicalDataset().getFormatSettings()));
    }

    if(datasetConfig.getReadDefinition() != null && datasetConfig.getReadDefinition().getSortColumnsList() != null) {
      options.add(new SortColumnsOption(datasetConfig.getReadDefinition().getSortColumnsList()));
    }

    BatchSchema schema = DatasetHelper.getSchemaBytes(datasetConfig) != null ? CalciteArrowHelper.fromDataset(datasetConfig) : null;
    if(schema != null) {
      options.add(new CurrentSchemaOption(schema));
    }

    if (datasetConfig.getReadDefinition() != null && datasetConfig.getReadDefinition().getExtendedProperty() != null) {
      options.add(new ExtendedPropertyOption(os -> ByteString.writeTo(os, datasetConfig.getReadDefinition().getExtendedProperty())));
    }

    List<T> addOptions = options.stream().filter(o -> clazz.isAssignableFrom(o.getClass())).map(o -> clazz.cast(o)).collect(Collectors.toList());
    outOptions.addAll(addOptions);
  }
}
