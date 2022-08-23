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
package com.dremio.service.nessie;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.AdjustableDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.GlobalLogCompactionParams;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

@ExtendWith({DatabaseAdapterExtension.class})
@NessieDbAdapterName(DatastoreDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(DatastoreTestConnectionProviderSource.class)
/*
 * These config values are not named very well:
 * https://github.com/projectnessie/nessie/blob/nessie-0.30.0/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/spi/AbstractDatabaseAdapter.java#L1075
 * https://github.com/projectnessie/nessie/blob/nessie-0.30.0/versioned/persist/adapter/src/main/java/org/projectnessie/versioned/persist/adapter/spi/AbstractDatabaseAdapter.java#L1088
 * In order to achieve our test scenario on 0.30 we have to set both of them at the same time.
 */
@NessieDbAdapterConfigItem(name = "max.key.list.size", value = "0") // prevent key lists inside commits
@NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "0") // prevent key lists inside commits
@NessieDbAdapterConfigItem(name = "key.list.distance", value = "20") // testKeyListPurge() depends on this value
class ITCommitLogMaintenance {

  @SuppressWarnings("checkstyle:visibilityModifier") // this field is set via reflection, it cannot be `private`
  @NessieDbAdapter(configMethod = "configureAdapter")
  protected static DatabaseAdapter databaseAdapter;

  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  private static final AtomicLong clockMillis = new AtomicLong();

  private static final Clock testClock = new Clock() {
    @Override
    public ZoneId getZone() {
      return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(clockMillis.get());
    }
  };

  @SuppressWarnings("unused") // invoked via reflection, see annotations on `databaseAdapter`
  static DatabaseAdapterConfig configureAdapter(AdjustableDatabaseAdapterConfig config) {
    return config.withClock(testClock);
  }

  private DatastoreDatabaseAdapter adapter() {
    return (DatastoreDatabaseAdapter) databaseAdapter;
  }

  private void put(Key key, long timestamp) throws ReferenceNotFoundException, ReferenceConflictException {
    IcebergTable table = IcebergTable.of(key.toString() + "-loc", 1, 2, 3, 4, UUID.randomUUID().toString());

    clockMillis.set(timestamp);
    ContentId contentId = ContentId.of(UUID.randomUUID().toString());
    adapter().commit(ImmutableCommitParams.builder()
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(worker.getMetadataSerializer().toBytes(CommitMeta.fromMessage("test-" + key)))
      .addPuts(KeyWithBytes.of(key, contentId, worker.getPayload(table),
        worker.toStoreOnReferenceState(table)))
      .putGlobal(contentId, worker.toStoreGlobalState(table))
      .build());
  }

  private void delete(Key key, long timestamp) throws ReferenceNotFoundException, ReferenceConflictException {
    clockMillis.set(timestamp);
    adapter().commit(ImmutableCommitParams.builder()
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(worker.getMetadataSerializer().toBytes(CommitMeta.fromMessage("del-" + key)))
      .addDeletes(key)
      .build());
  }

  @BeforeEach
  void resetRepo() throws ReferenceNotFoundException, ReferenceConflictException {
    ReferenceInfo<ByteString> main = adapter().namedRef("main", GetNamedRefsParams.DEFAULT);
    adapter().assign(main.getNamedRef(), Optional.empty(), adapter().noAncestorHash());
  }

  private void validateActiveKeys(Collection<Key> activeKeys) throws ReferenceNotFoundException {
    ReferenceInfo<ByteString> main = adapter().namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(adapter().keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL).map(KeyListEntry::getKey))
      .containsExactlyInAnyOrderElementsOf(activeKeys);

    Map<Key, ContentAndState<ByteString>> values = adapter()
      .values(main.getHash(), activeKeys, KeyFilterPredicate.ALLOW_ALL);

    assertThat(values).hasSize(activeKeys.size());
    activeKeys.forEach(k -> {
      ContentAndState<ByteString> value = values.get(k);
      ByteString refState = value.getRefState();
      IcebergTable table = (IcebergTable) worker.valueFromStore(refState, value::getGlobalState);
      assertThat(table.getMetadataLocation()).isEqualTo(k.toString() + "-loc");
    });
  }

  @Test
  void testKeyListPurge() throws ReferenceNotFoundException, ReferenceConflictException {
    Set<Key> activeKeys = new HashSet<>();

    // Generate commits to have one key list
    for (int i = 0; i < 20; i++) {
      Key key = Key.of("test-" + i);
      put(key, 100);
      activeKeys.add(key);
    }
    clockMillis.set(150);

    EmbeddedRepoMaintenanceParams params = EmbeddedRepoMaintenanceParams.builder()
      .setGlobalLogCompactionParams(GlobalLogCompactionParams.builder().isEnabled(false).build())
      .setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder().build())
      .build();

    Map<String, Map<String, String>> result = adapter().repoMaintenance(params);
    assertThat(result.get("purgeKeyLists")).isEqualTo(ImmutableMap.of(
      "duration", "PT0S", // The test clock does not advance during repoMaintenance()
      // The twentieth commit has one own key plus 19 keys from previous commits, each occupying a separate entity
      // because the `max.key.list.size` parameter is set to 0.
      "liveKeyListEntities", "19",
      "obsoleteKeyListEntities", "0", // There are no key lists to purge
      "deletedKeyListEntities", "0",
      "processedCommits", "20",
      "updatedCommits", "0" // There are no key lists to purge
    ));

    // Delete some old keys, but do not cause a new key list to be generated
    for (int i = 0; i < 8; i++) {
      Key key = Key.of("test-" + i);
      delete(key, 200);
      activeKeys.remove(key);
    }
    clockMillis.set(250);

    result = adapter().repoMaintenance(params);
    assertThat(result.get("purgeKeyLists")).isEqualTo(ImmutableMap.of(
      "duration", "PT0S", // The test clock does not advance during repoMaintenance()
      // No new key lists are produced
      "liveKeyListEntities", "19",
      "obsoleteKeyListEntities", "0", // There are no key lists to purge
      "deletedKeyListEntities", "0",
      "processedCommits", "28",
      "updatedCommits", "0" // There are no key lists to purge
    ));

    validateActiveKeys(activeKeys);

    // Add more keys to cause a new key list to be generated
    for (int i = 0; i < 12; i++) {
      Key key = Key.of("test2-" + i);
      put(key, 300);
      activeKeys.add(key);
    }
    clockMillis.set(350);

    result = adapter().repoMaintenance(params);
    assertThat(result.get("purgeKeyLists")).isEqualTo(ImmutableMap.of(
      "duration", "PT0S", // The test clock does not advance during repoMaintenance()
      // The latest 20 commits deleted 8 old keys and added 12 new keys.
      // All keys except one are kept in a new key list. The commit itself holds one (latest) key.
      "liveKeyListEntities", "23",
      "obsoleteKeyListEntities", "19", // the first key list is obsolete now
      "deletedKeyListEntities", "19",
      "processedCommits", "40",
      "updatedCommits", "1" // One key list was dropped, referenced from one commit
    ));

    validateActiveKeys(activeKeys);

    // Delete all keys from the first key list (some of them for the second time)
    for (int i = 0; i < 20; i++) {
      Key key = Key.of("test-" + i);
      delete(key, 400);
      activeKeys.remove(key);
    }
    clockMillis.set(450);

    result = adapter().repoMaintenance(params);
    assertThat(result.get("purgeKeyLists")).isEqualTo(ImmutableMap.of(
      "duration", "PT0S", // The test clock does not advance during repoMaintenance()
      // The latest key list is kept (12 "test2-*" keys + "test-19", which is a feature of Nessie, cf. GH Issue #4772
      "liveKeyListEntities", "13",
      "obsoleteKeyListEntities", "23", // the second key list is obsolete now
      "deletedKeyListEntities", "23",
      "processedCommits", "60",
      "updatedCommits", "1" // One key list was dropped, referenced from one commit
    ));

    validateActiveKeys(activeKeys);
  }

  @Test
  void testDryRun() throws ReferenceNotFoundException, ReferenceConflictException {
    // Generate commits to have two key lists
    for (int i = 0; i < 40; i++) {
      Key key = Key.of("test-" + i);
      put(key, 100);
    }
    clockMillis.set(150);

    EmbeddedRepoMaintenanceParams params = EmbeddedRepoMaintenanceParams.builder()
      .setGlobalLogCompactionParams(GlobalLogCompactionParams.builder().isEnabled(false).build())
      .setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder().setDryRun(true).build())
      .build();

    Map<String, Map<String, String>> result = adapter().repoMaintenance(params);
    assertThat(result.get("purgeKeyLists")).isEqualTo(ImmutableMap.of(
      "duration", "PT0S", // The test clock does not advance during repoMaintenance()
      // The fortieth commit has one own key plus 39 keys from previous commits, each occupying a separate entity
      // because the `max.key.list.size` parameter is set to 0.
      "liveKeyListEntities", "39",
      "obsoleteKeyListEntities", "19", // the key list from the twentieth commit is obsolete
      "deletedKeyListEntities", "0", // dry run
      "processedCommits", "40",
      "updatedCommits", "0" // dry run
    ));
  }

  @Test
  void testProgress() throws ReferenceNotFoundException, ReferenceConflictException {
    // Generate commits to have three key lists
    for (int i = 0; i < 60; i++) {
      Key key = Key.of("test-" + i);
      put(key, 100);
    }
    clockMillis.set(150);

    AtomicInteger commits = new AtomicInteger();
    AtomicInteger keyListEntities = new AtomicInteger();
    EmbeddedRepoMaintenanceParams params = EmbeddedRepoMaintenanceParams.builder()
      .setGlobalLogCompactionParams(GlobalLogCompactionParams.builder().isEnabled(false).build())
      .setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder()
        .setProgressReporter(new EmbeddedRepoPurgeParams.ProgressConsumer() {
          @Override
          public void onCommitProcessed(Hash hash) {
            commits.incrementAndGet();
          }

          @Override
          public void onKeyListEntityDeleted(Hash hash) {
            keyListEntities.incrementAndGet();
          }
        })
        .build())
      .build();

    Map<String, Map<String, String>> result = adapter().repoMaintenance(params);
    assertThat(commits.get()).isEqualTo(60);
    // 19 entries from the first obsolete key list, 39 from the second
    assertThat(keyListEntities.get()).isEqualTo(19 + 39);
  }
}
