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

import static com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_CURRENT_VERSION_ONLY;
import static com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy.KEEP_VALID_SPLITS;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.service.namespace.PartitionChunkId.SplitOrphansRetentionPolicy;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.collect.Range;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Test the various split retention policies */
@RunWith(Parameterized.class)
public class TestSplitOrphansRetentionPolicy {
  private static final MetadataPolicy SOURCE_METADATA_POLICY =
      new MetadataPolicy().setDatasetDefinitionExpireAfterMs(TimeUnit.DAYS.toMillis(1));
  private static final MetadataPolicy SOURCE_METADATA_POLICY1 =
      new MetadataPolicy()
          .setDatasetDefinitionRefreshAfterMs(TimeUnit.HOURS.toMillis(1))
          .setDatasetDefinitionExpireAfterMs(TimeUnit.HOURS.toMillis(3));
  private static final MetadataPolicy SOURCE_METADATA_POLICY2 =
      new MetadataPolicy()
          .setDatasetDefinitionRefreshAfterMs(TimeUnit.MINUTES.toMillis(1))
          .setDatasetDefinitionExpireAfterMs(TimeUnit.HOURS.toMillis(6));
  private static final MetadataPolicy SOURCE_METADATA_POLICY3 =
      new MetadataPolicy()
          .setDatasetDefinitionRefreshAfterMs(TimeUnit.DAYS.toMillis(1))
          .setDatasetDefinitionExpireAfterMs(TimeUnit.DAYS.toMillis(100));

  private static final EntityId DATASET_ID = new EntityId(UUID.randomUUID().toString());
  private static final long TS_NOW = System.currentTimeMillis();

  private static final SplitOrphansRetentionPolicy KEEP_VALID_SPLITS_WITH_SMART_EXPIRY =
      new SplitOrphansRetentionPolicy.SmartExpirationPolicyForSplits(3);

  private static final DatasetConfig DATASET =
      new DatasetConfig()
          .setId(DATASET_ID)
          .setFullPathList(Arrays.asList("source", "dataset"))
          .setReadDefinition(new ReadDefinition().setSplitVersion(TS_NOW));

  private static final EntityId OLD_DATASET_ID = new EntityId(UUID.randomUUID().toString());
  private static final long OLD_DATASET_SPLIT_VERSION = TS_NOW - TimeUnit.DAYS.toMillis(2);
  private static final long NOW_MINUS_3_HOURS = TS_NOW - TimeUnit.HOURS.toMillis(3);
  private static final long NOW_MINUS_4_HOURS = TS_NOW - TimeUnit.HOURS.toMillis(4);
  private static final long NOW_MINUS_2_DAYS = TS_NOW - TimeUnit.DAYS.toMillis(2);

  private static final DatasetConfig OLD_DATASET =
      new DatasetConfig()
          .setId(OLD_DATASET_ID)
          .setFullPathList(Arrays.asList("source", "old_dataset"))
          .setReadDefinition(new ReadDefinition().setSplitVersion(OLD_DATASET_SPLIT_VERSION));

  @Parameters
  public static Iterable<Object[]> getTestCases() {
    return Arrays.asList(
        // Current version policy
        newTestCase(
            true,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - 1, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW + 1, "foo")),
        newTestCase(
            true,
            KEEP_CURRENT_VERSION_ONLY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - 1, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW + 1, "foo")),
        newTestCase(
            true,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION - 1, "foo")),
        newTestCase(
            false,
            KEEP_CURRENT_VERSION_ONLY,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION + 1, "foo")),

        // Valid splits policy
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW + 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID,
                TS_NOW - TimeUnit.DAYS.toMillis(1) + TimeUnit.SECONDS.toMillis(30),
                "foo")),
        // The one below is most likely already expired, but maybe it will take less than 1millis
        // (or whatever is the resolution of System.currentTimeMillis())
        // newTestCase(true, KEEP_VALID_SPLITS, SOURCE, DATASET, DatasetSplitId.of(DATASET_ID,
        // TS_NOW - TimeUnit.DAYS.toMillis(1), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - TimeUnit.DAYS.toMillis(1) - 1, "foo")),
        // if metadata policy is null, nothing expires
        newTestCase(
            true, KEEP_VALID_SPLITS, null, DATASET, PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW + 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            null,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID,
                TS_NOW - TimeUnit.DAYS.toMillis(1) + TimeUnit.SECONDS.toMillis(30),
                "foo")),
        newTestCase(
            true, KEEP_VALID_SPLITS, null, DATASET, PartitionChunkId.of(DATASET_ID, 0, "foo")),
        // If old datasets expired, still keep the current version (or higher as it is a range...)
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION, "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION - 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION + 1, "foo")),

        // Valid splits with auto expire policy
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY1,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_3_HOURS + TimeUnit.SECONDS.toMillis(30), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY1,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_3_HOURS - TimeUnit.SECONDS.toMillis(30), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY2,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_4_HOURS + TimeUnit.SECONDS.toMillis(30), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY2,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_4_HOURS - TimeUnit.SECONDS.toMillis(30), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY3,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_2_DAYS + TimeUnit.SECONDS.toMillis(30), "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY3,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID, NOW_MINUS_2_DAYS - TimeUnit.SECONDS.toMillis(30), "foo")),
        // if metadata policy is null, nothing expires
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW - 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, TS_NOW + 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            null,
            DATASET,
            PartitionChunkId.of(
                DATASET_ID,
                TS_NOW - TimeUnit.DAYS.toMillis(1) + TimeUnit.SECONDS.toMillis(30),
                "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            null,
            DATASET,
            PartitionChunkId.of(DATASET_ID, 0, "foo")),
        // If old datasets expired, still keep the current version (or higher as it is a range...)
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION, "foo")),
        newTestCase(
            false,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY1,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION - 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY2,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION + 1, "foo")),
        newTestCase(
            true,
            KEEP_VALID_SPLITS_WITH_SMART_EXPIRY,
            SOURCE_METADATA_POLICY3,
            OLD_DATASET,
            PartitionChunkId.of(OLD_DATASET_ID, OLD_DATASET_SPLIT_VERSION + 1, "foo")));
  }

  private static Object[] newTestCase(
      boolean expected,
      SplitOrphansRetentionPolicy policy,
      MetadataPolicy metadataPolicy,
      DatasetConfig config,
      PartitionChunkId splitId) {
    return new Object[] {expected, policy, metadataPolicy, config, splitId};
  }

  private final boolean expected;
  private final SplitOrphansRetentionPolicy policy;
  private final MetadataPolicy metadataPolicy;
  private final DatasetConfig config;
  private final PartitionChunkId splitId;

  public TestSplitOrphansRetentionPolicy(
      boolean expected,
      SplitOrphansRetentionPolicy policy,
      MetadataPolicy metadataPolicy,
      DatasetConfig config,
      PartitionChunkId splitId) {
    this.expected = expected;
    this.policy = policy;
    this.metadataPolicy = metadataPolicy;
    this.config = config;
    this.splitId = splitId;
  }

  @Test
  public void checkRetention() {
    Range<PartitionChunkId> range = policy.apply(metadataPolicy, config);

    assertThat(range.contains(splitId)).isEqualTo(expected);
  }
}
