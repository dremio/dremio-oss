/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Dataset Split key
 */
public class DatasetSplitId {
  private static final String DELIMITER = "_";
  private static final Joiner SPLIT_ID_JOINER = Joiner.on(DELIMITER);
  private static final String EMPTY = "";
  private final String datasetId;
  private final String splitIdentifier;
  private final String splitId;

  public DatasetSplitId(DatasetConfig config, DatasetSplit split, long splitVersion) {
    Preconditions.checkArgument(splitVersion > -1);
    this.datasetId = config.getId().getId();
    this.splitIdentifier = split.getSplitKey();
    this.splitId = SPLIT_ID_JOINER.join(datasetId, splitVersion, splitIdentifier);
  }

  @JsonCreator
  public DatasetSplitId(String datasetSplitId) throws IllegalArgumentException {
    final String[] ids = datasetSplitId.split(DELIMITER, 3);
    if (ids.length != 3 || ids[0].isEmpty() || ids[1].isEmpty() || ids[2].isEmpty()) {
      throw new IllegalArgumentException("Invalid dataset split id " + datasetSplitId);
    }
    this.datasetId = ids[0];
    this.splitIdentifier = ids[2];
    this.splitId = datasetSplitId;
  }

  // used for range query on kvstore
  private DatasetSplitId(String datasetId, String splitId) {
    this.datasetId = datasetId;
    this.splitId = splitId;
    this.splitIdentifier = EMPTY;
  }

  @JsonValue
  public String getSpiltId() {
    return splitId;
  }

  @JsonIgnore
  public String getDatasetId() {
    return datasetId;
  }

  @JsonIgnore
  public String getSplitIdentifier() {
    return splitIdentifier;
  }

  @Override
  public int hashCode() {
    return splitId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof DatasetSplitId) {
        final DatasetSplitId other = (DatasetSplitId)obj;
        return splitId.equals(other.splitId);
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return splitId;
  }

  public static SearchQuery getSplitsQuery(DatasetConfig datasetConfig) {
    Preconditions.checkNotNull(datasetConfig.getReadDefinition());
    Preconditions.checkNotNull(datasetConfig.getReadDefinition().getSplitVersion());
    return SearchQueryUtils.and(
      SearchQueryUtils.newTermQuery(DATASET_ID, datasetConfig.getId().getId()),
      SearchQueryUtils.newTermQuery(SPLIT_VERSION.getIndexFieldName(), datasetConfig.getReadDefinition().getSplitVersion()));
  }

  public static FindByRange<DatasetSplitId> getAllSplitsRange(DatasetConfig datasetConfig) {
    final long splitVersion = datasetConfig.getReadDefinition().getSplitVersion();
    final long nextSplitVersion = splitVersion + 1;
    final String datasetId = datasetConfig.getId().getId();

    final DatasetSplitId start = new DatasetSplitId(datasetId, String.format("%s%s%d%s", datasetId, DELIMITER, 0, DELIMITER));
    final DatasetSplitId end = new DatasetSplitId(datasetId, String.format("%s%s%d%s", datasetId, DELIMITER, nextSplitVersion, DELIMITER));

    return new FindByRange<DatasetSplitId>()
      .setStart(start, true)
      .setEnd(end, false);
  }

  public static FindByRange<DatasetSplitId> getSplitsRange(DatasetConfig datasetConfig) {
    final long splitVersion = datasetConfig.getReadDefinition().getSplitVersion();
    final long nextSplitVersion = splitVersion + 1;
    final String datasetId = datasetConfig.getId().getId();
    // create start and end id with empty split identifier
    final DatasetSplitId start = new DatasetSplitId(datasetId, String.format("%s%s%d%s", datasetId, DELIMITER, splitVersion, DELIMITER));
    final DatasetSplitId end = new DatasetSplitId(datasetId, String.format("%s%s%d%s", datasetId, DELIMITER, nextSplitVersion, DELIMITER));

    return new FindByRange<DatasetSplitId>()
      .setStart(start, true)
      .setEnd(end, false);
  }
}
