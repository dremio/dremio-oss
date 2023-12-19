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
package com.dremio.exec.ops;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.store.TableMetadata;
import com.google.common.base.Preconditions;

/**
 * Class for storing all the needed information to
 * do a diff between multiple Iceberg Snapshots.
 * We support diff based on
 * 1) Modified Files with multiple Intervals supported
 * 2) Modified Files Raised to Partitions level with single Interval supported
 * (meaning all data for any partition containing a modified file)
 */
public class SnapshotDiffContext implements Cloneable {
  public static final SnapshotDiffContext NO_SNAPSHOT_DIFF = new SnapshotDiffContext();
  private PartitionSpec reflectionPartitionSpec; //target partition spec to raise to for the reflection
  private PartitionSpec baseDatasetTargetPartitionSpec;//target partition spec to raise to for the base dataset

  private List<SnapshotDiffSingleInterval> intervals;

  private FilterApplyOptions filterApplyOptions;

  private RelNode deleteFilesFilter;

  public enum FilterApplyOptions {
    NO_FILTER,
    FILTER_DATA_FILES,
    FILTER_PARTITIONS
  }

  public SnapshotDiffContext(final TableMetadata beginningTableMetadata,final TableMetadata endingTableMetadata,final FilterApplyOptions filterApplyOptions) {
    if (filterApplyOptions == null || beginningTableMetadata == null || endingTableMetadata == null) {
      new SnapshotDiffContext();
    } else {
      this.intervals = new ArrayList<>();
      intervals.add(new SnapshotDiffSingleInterval(beginningTableMetadata,endingTableMetadata));
      this.filterApplyOptions = filterApplyOptions;
    }
  }

  public SnapshotDiffContext() {
    this.intervals = new ArrayList<>();
    this.filterApplyOptions = FilterApplyOptions.NO_FILTER;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    //We do not allow cloning this class
    //A single SnapshotDiffContext should be used throughout the whole query plan
    throw new CloneNotSupportedException();
  }

  public RelNode getDeleteFilesFilter() {
    return deleteFilesFilter;
  }

  public void setDeleteFilesFilter(final RelNode prel){
    deleteFilesFilter = prel;
  }

  public void setReflectionTargetPartitionSpec(final PartitionSpec spec) {
    this.reflectionPartitionSpec = spec;
  }

  public PartitionSpec getReflectionPartitionSpec() {
    return reflectionPartitionSpec;
  }


  public void setBaseDatasetTargetPartitionSpec(final PartitionSpec baseDatasetTargetPartitionSpec) {
    this.baseDatasetTargetPartitionSpec = baseDatasetTargetPartitionSpec;
  }

  public PartitionSpec getBaseDatasetTargetPartitionSpec() {
    return baseDatasetTargetPartitionSpec;
  }

  public void addSnapshotDiffInterval(final TableMetadata beginningTableMetadata, final TableMetadata endingTableMetadata){
    intervals.add(new SnapshotDiffSingleInterval(beginningTableMetadata,endingTableMetadata));
  }

  public FilterApplyOptions getFilterApplyOptions() {
    return filterApplyOptions;
  }

  public void setFilterApplyOptions(final FilterApplyOptions filterApplyOptions) {
    this.filterApplyOptions = filterApplyOptions;
  }

  public boolean isEnabled(){
    return filterApplyOptions != FilterApplyOptions.NO_FILTER
      && !intervals.isEmpty();
  }

  public boolean isSameSnapshot(){
    return intervals.size() == 1 &&
      intervals.get(0).getBeginningTableMetadata().equals(intervals.get(0).getEndingTableMetadata());
  }

  public List<SnapshotDiffSingleInterval> getIntervals() {
    return intervals;
  }

  public static class SnapshotDiffSingleInterval{
    private final TableMetadata beginningTableMetadata;
    private final TableMetadata endingTableMetadata;

    SnapshotDiffSingleInterval(final TableMetadata beginningTableMetadata,final TableMetadata endingTableMetadata){
      this.beginningTableMetadata = Preconditions.checkNotNull(beginningTableMetadata);
      this.endingTableMetadata = Preconditions.checkNotNull(endingTableMetadata);
    }

    public TableMetadata getEndingTableMetadata() {
      return endingTableMetadata;
    }

    public TableMetadata getBeginningTableMetadata() {
      return beginningTableMetadata;
    }
  }
}
