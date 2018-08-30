/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.DatasetSplitIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_VERSION;

import com.dremio.datastore.KVStore.FindByRange;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.proto.EntityId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

/**
 * Dataset Split key
 */
public class DatasetSplitId {
  private static final String DELIMITER = "_";
  private static final Joiner SPLIT_ID_JOINER = Joiner.on(DELIMITER);
  private static final String EMPTY = "";
  private final String datasetId;
  private final String splitKey;
  private final String compoundSplitId;

  public DatasetSplitId(DatasetConfig config, DatasetSplit split, long splitVersion) {
    Preconditions.checkArgument(splitVersion > -1);
    this.datasetId = config.getId().getId();
    this.splitKey = split.getSplitKey();
    this.compoundSplitId = SPLIT_ID_JOINER.join(datasetId, splitVersion, splitKey);
  }

  @JsonCreator
  public DatasetSplitId(String datasetSplitId) throws IllegalArgumentException {
    final String[] ids = datasetSplitId.split(DELIMITER, 3);
    if (ids.length != 3 || ids[0].isEmpty() || ids[1].isEmpty() || ids[2].isEmpty()) {
      throw new IllegalArgumentException("Invalid dataset split id " + datasetSplitId);
    }
    this.datasetId = ids[0];

    this.splitKey = ids[2];
    this.compoundSplitId = datasetSplitId;
  }

  // used for range query on kvstore
  private DatasetSplitId(String datasetId, String splitId) {
    this.datasetId = datasetId;
    this.compoundSplitId = splitId;
    this.splitKey = EMPTY;
  }

  @JsonValue
  public String getSplitId() {
    return compoundSplitId;
  }

  @JsonIgnore
  public String getDatasetId() {
    return datasetId;
  }

  @JsonIgnore
  public String getSplitIdentifier() {
    return splitKey;
  }

  @Override
  public int hashCode() {
    return compoundSplitId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof DatasetSplitId) {
        final DatasetSplitId other = (DatasetSplitId)obj;
        return compoundSplitId.equals(other.compoundSplitId);
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return compoundSplitId;
  }

  public static SearchQuery getSplitsQuery(DatasetConfig datasetConfig) {
    Preconditions.checkNotNull(datasetConfig.getReadDefinition());
    long splitVersion = Preconditions.checkNotNull(datasetConfig.getReadDefinition().getSplitVersion());
    return getSplitsQuery(datasetConfig.getId(), splitVersion);
  }

  public static SearchQuery getSplitsQuery(EntityId datasetId, long splitVersion) {
    return SearchQueryUtils.and(
      SearchQueryUtils.newTermQuery(DATASET_ID, datasetId.getId()),
      SearchQueryUtils.newTermQuery(SPLIT_VERSION, splitVersion));
  }

  public static Range<String> getSplitStringRange(DatasetConfig datasetConfig){
    final long splitVersion = datasetConfig.getReadDefinition().getSplitVersion();
    final long nextSplitVersion = splitVersion + 1;
    final String datasetId = datasetConfig.getId().getId();

    // create start and end id with empty split identifier
    final String start = getStringId(datasetId, splitVersion);
    final String end = getStringId(datasetId, nextSplitVersion);
    return Range.closedOpen(start, end);
  }

  public static Range<String> getSplitStringRange(EntityId datasetId, long splitVersion) {
    final long nextSplitVersion = splitVersion + 1;

    // create start and end id with empty split identifier
    final String start = getStringId(datasetId.getId(), splitVersion);
    final String end = getStringId(datasetId.getId(), nextSplitVersion);
    return Range.closedOpen(start, end);
  }

  public static FindByRange<DatasetSplitId> getAllSplitsRange(DatasetConfig datasetConfig) {
    final long splitVersion = datasetConfig.getReadDefinition().getSplitVersion();
    final long nextSplitVersion = splitVersion + 1;
    final String datasetId = datasetConfig.getId().getId();

    final DatasetSplitId start = getId(datasetId, 0);
    final DatasetSplitId end = getId(datasetId, nextSplitVersion);

    return new FindByRange<DatasetSplitId>()
      .setStart(start, true)
      .setEnd(end, false);
  }

  private static String getStringId(String datasetId, long version) {
    return String.format("%s%s%d%s", datasetId, DELIMITER, version, DELIMITER);
  }

  private static DatasetSplitId getId(String datasetId, long version) {
    return new DatasetSplitId(datasetId, getStringId(datasetId, version));
  }

  public static FindByRange<DatasetSplitId> getSplitsRange(DatasetConfig datasetConfig) {
    final String datasetId = datasetConfig.getId().getId();
    Range<String> range = getSplitStringRange(datasetConfig);
    final DatasetSplitId start = new DatasetSplitId(datasetId, range.lowerEndpoint());
    final DatasetSplitId end = new DatasetSplitId(datasetId, range.upperEndpoint());

    return new FindByRange<DatasetSplitId>()
      .setStart(start, true)
      .setEnd(end, false);
  }

  public static FindByRange<DatasetSplitId> getSplitsRange(EntityId datasetId, long splitVersionId) {
    Range<String> range = getSplitStringRange(datasetId, splitVersionId);
    final DatasetSplitId start = new DatasetSplitId(datasetId.getId(), range.lowerEndpoint());
    final DatasetSplitId end = new DatasetSplitId(datasetId.getId(), range.upperEndpoint());

    return new FindByRange<DatasetSplitId>()
      .setStart(start, true)
      .setEnd(end, false);
  }
}
