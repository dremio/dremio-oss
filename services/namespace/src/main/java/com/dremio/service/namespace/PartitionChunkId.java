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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.DatasetSplitIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_VERSION;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Optional;

import javax.annotation.Nullable;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.api.LegacyKVStore.LegacyFindByRange;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

/**
 * Dataset Split key
 */
public final class PartitionChunkId implements Comparable<PartitionChunkId> {
  // Private comparator
  private static final Comparator<PartitionChunkId> COMPARATOR = Comparator
      .comparing(PartitionChunkId::getDatasetId)
      .thenComparing(PartitionChunkId::getSplitVersion)
      .thenComparing(PartitionChunkId::getSplitIdentifier);

  // DO NOT USE DIRECTLY unless you understand exactly how PartitionChunkId is encoded
  // The encoding is <encoded dataset id>_<version>_<key>
  // - dataset id should be encoded so that underscores do not appear
  // - version should be a number only
  // - key has no special restriction
  private static final String DELIMITER = "_";
  private static final Joiner SPLIT_ID_JOINER = Joiner.on(DELIMITER);
  private static final CharMatcher RESERVED_DATASET_ID_CHARACTERS = CharMatcher.anyOf("_%");

  private final String datasetId;
  private final long splitVersion;
  private final String splitKey;
  private final String compoundSplitId;

  @JsonCreator
  public static PartitionChunkId of(String partitionChunkId) {
    final String[] ids = partitionChunkId.split(DELIMITER, 3);
    Preconditions.checkArgument(ids.length == 3 && !ids[0].isEmpty() && !ids[1].isEmpty() && !ids[2].isEmpty(),
        "Invalid dataset split id %s", partitionChunkId);

    // Some dataset split before upgrade might not have a valid version
    // but the compound key would still allow for the entry to be removed from the kvstore
    // so allowing it temporarily.
    //
    // See DX-13336 for details
    long version;
    try {
      version = Long.parseLong(ids[1]);
    } catch (NumberFormatException e) {
      version = Long.MIN_VALUE;
    }
    return new PartitionChunkId(partitionChunkId, unescape(ids[0]), version, ids[2]);
  }

  public static PartitionChunkId of(DatasetConfig config, PartitionChunk split, long splitVersion) {
    Preconditions.checkArgument(splitVersion > -1);
    EntityId datasetId = config.getId();
    String splitKey = split.getSplitKey();

    return of(datasetId, splitVersion, splitKey);
  }

  public static PartitionChunkId of(DatasetConfig config, PartitionChunkMetadata partitionChunkMetadata, long splitVersion) {
    Preconditions.checkArgument(splitVersion > -1);
    EntityId datasetId = config.getId();
    String splitKey = partitionChunkMetadata.getSplitKey();

    return of(datasetId, splitVersion, splitKey);
  }


  private static String escape(String datasetId) {
    // Replace % and _ with their URL encoded counter parts
    // Order is important (first encode % and then _) as
    StringBuilder sb = new StringBuilder(datasetId.length());
    for(int i = 0; i < datasetId.length(); i++) {
      char c = datasetId.charAt(i);
      switch(c) {
      case '%':
        sb.append("%25");
        break;

      case '_':
        sb.append("%5F");
        break;

      default:
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static String unescape(String encoded) {
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      // should never happen as UTF-8 should always be supported
      throw new AssertionError(e);
    }
  }


  @VisibleForTesting
  static PartitionChunkId of(EntityId datasetId, long splitVersion, String splitKey) {
    final String datasetIdAsString = escape(datasetId.getId());
    String compoundSplitId = SPLIT_ID_JOINER.join(datasetIdAsString, splitVersion, splitKey);

    return new PartitionChunkId(compoundSplitId, datasetIdAsString, splitVersion, splitKey);
  }

  // To be only used for testing migration of unsafe ids
  @VisibleForTesting
  static PartitionChunkId ofUnsafe(EntityId datasetId, long splitVersion, String splitKey) {
    final String datasetIdAsString = datasetId.getId();
    String compoundSplitId = SPLIT_ID_JOINER.join(datasetIdAsString, splitVersion, splitKey);

    return new PartitionChunkId(compoundSplitId, datasetIdAsString, splitVersion, splitKey);
  }
  private PartitionChunkId(String compoundSplitId, String datasetId, long splitVersion, String splitKey) {
    this.datasetId = datasetId;
    this.splitVersion = splitVersion;
    this.splitKey = splitKey;
    this.compoundSplitId = compoundSplitId;
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
  public long getSplitVersion() {
    return splitVersion;
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
      if (obj instanceof PartitionChunkId) {
        final PartitionChunkId other = (PartitionChunkId)obj;
        return compoundSplitId.equals(other.compoundSplitId);
      }
    }
    return false;
  }

  @Override
  public int compareTo(PartitionChunkId that) {
    return COMPARATOR.compare(this, that);
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

  /**
   * Create a range for the current split version of the given dataset
   *
   * @param datasetConfig the dataset config
   * @return a range which would contain all split ids for this dataset and its current split version
   */
  public static Range<PartitionChunkId> getCurrentSplitRange(DatasetConfig datasetConfig){
    final long splitVersion = datasetConfig.getReadDefinition().getSplitVersion();

    return getSplitRange(datasetConfig.getId(), splitVersion);
  }

  /**
   * Create a range for the given split version of the dataset
   *
   * @param datasetId the dataset id
   * @param splitVersion the split version
   * @return a range which would contain all split ids for this dataset and the split version
   */
  public static Range<PartitionChunkId> getSplitRange(EntityId datasetId, long splitVersion) {
    return getSplitRange(datasetId, splitVersion, splitVersion + 1);
  }

  /**
   * Create a range for partition chunk id
   *
   * Create a range to check if a split id belongs to the provided dataset id and its version is comprised
   * into startSplitVersion (included) and endSplitVersion (excluded).
   *
   * @param datasetId the dataset id
   * @param startSplitVersion the minimum split version
   * @param endSplitVersion the maximum split version
   * @return
   */
  public static Range<PartitionChunkId> getSplitRange(EntityId datasetId, long startSplitVersion, long endSplitVersion) {
    // create start and end id with empty split identifier
    final PartitionChunkId start = getId(datasetId, startSplitVersion);
    final PartitionChunkId end = getId(datasetId, endSplitVersion);
    return Range.closedOpen(start, end);
  }

  private static PartitionChunkId getId(EntityId datasetId, long version) {
    return PartitionChunkId.of(datasetId, version, "");
  }

  public static LegacyFindByRange<PartitionChunkId> getSplitsRange(DatasetConfig datasetConfig) {
    Range<PartitionChunkId> range = getCurrentSplitRange(datasetConfig);

    return new LegacyFindByRange<PartitionChunkId>()
      .setStart(range.lowerEndpoint(), true)
      .setEnd(range.upperEndpoint(), false);
  }

  public static LegacyFindByRange<PartitionChunkId> getSplitsRange(EntityId datasetId, long splitVersionId) {
    Range<PartitionChunkId> range = getSplitRange(datasetId, splitVersionId);

    return new LegacyFindByRange<PartitionChunkId>()
      .setStart(range.lowerEndpoint(), true)
      .setEnd(range.upperEndpoint(), false);
  }

  /**
   * Check if split id for this dataset may need new split id
   *
   * See DX-13336 for details
   *
   * @param config the dataset config
   * @return true if this dataset might be using legacy/invalid datasetId.
   */
  public static boolean mayRequireNewDatasetId(DatasetConfig config) {
    return RESERVED_DATASET_ID_CHARACTERS.matchesAnyOf(config.getId().getId());
  }

  /**
   * UNSAFE! Use {@code PartitionChunkId#getSplitRange(EntityId, long)} instead

   */
  public static LegacyFindByRange<PartitionChunkId> unsafeGetSplitsRange(DatasetConfig config) {
    final long splitVersion = config.getReadDefinition().getSplitVersion();
    final long nextSplitVersion = splitVersion + 1;
    final String datasetId = config.getId().getId();

    // Unsafe way of constructing dataset split id!!!
    final PartitionChunkId start = new PartitionChunkId(SPLIT_ID_JOINER.join(datasetId, splitVersion, ""), datasetId, splitVersion, "");
    final PartitionChunkId end = new PartitionChunkId(SPLIT_ID_JOINER.join(datasetId, nextSplitVersion, ""), datasetId, splitVersion, "");

    return new LegacyFindByRange<PartitionChunkId>()
      .setStart(start, true)
      .setEnd(end, false);
  }
  /**
   * Policy to get valid ranges of splits for a given dataset
   */
  @FunctionalInterface
  public interface SplitOrphansRetentionPolicy {
    /**
     * Only keep splits for a dataset matching the current dataset's split version
     */
    public static final SplitOrphansRetentionPolicy KEEP_CURRENT_VERSION_ONLY = (metadataPolicy, dataset) -> getCurrentSplitRange(dataset);

    /**
     * Only keep splits whose version is not expired based on the source metadata expiration policy.
     *
     * Keep current version, even if expired
     */
    public static final SplitOrphansRetentionPolicy KEEP_VALID_SPLITS = (metadataPolicy, dataset) -> {
      long expirationMs = Optional.ofNullable(metadataPolicy).map(MetadataPolicy::getDatasetDefinitionExpireAfterMs).orElse(Long.MAX_VALUE);

      // Anything less than expiredVersion is considered expired
      long expiredVersion = Math.max(0, System.currentTimeMillis() - expirationMs);

      // Make sure current version (or higher) is still valid. Some sources don't change split version if dataset is refreshed
      // (assuming split info do not change)
      long minVersion = Math.min(dataset.getReadDefinition().getSplitVersion(), expiredVersion);

      return getSplitRange(dataset.getId(), minVersion, Long.MAX_VALUE);
    };

    /**
     * Compute valid range of splits for a given dataset
     *
     * Compute a range of split ids which would be valid for the provided
     * metadata policy and dataset config
     *
     * @param metadataPolicy the metadata policy for the given dataset
     * @param dataset the dataset config
     * @return a range of valid split ids
     */
    Range<PartitionChunkId> apply(@Nullable MetadataPolicy metadataPolicy, DatasetConfig dataset);
  }
}
