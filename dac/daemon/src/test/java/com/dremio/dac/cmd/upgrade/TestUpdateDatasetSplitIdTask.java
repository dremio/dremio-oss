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
package com.dremio.dac.cmd.upgrade;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.junit.Test;

import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.UnsafeDatasetSplitIdHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.test.DremioTest;

/**
 * Test class for {@code UpdateDatasetSplitIdTask}
 */
public class TestUpdateDatasetSplitIdTask extends DremioTest {

  @Test
  public void test() throws Exception {
    try (final KVStoreProvider kvStoreProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvStoreProvider.start();
      final IndexedStore<byte[], NameSpaceContainer> namespace = kvStoreProvider.getStore(NamespaceServiceImpl.NamespaceStoreCreator.class);
      final IndexedStore<DatasetSplitId, DatasetSplit> splitsStore = kvStoreProvider.getStore(NamespaceServiceImpl.DatasetSplitCreator.class);

      final DatasetConfig ds1 = addDataset(namespace, splitsStore, "foo_bar", Arrays.asList("test", "foo_bar"), 10);
      final DatasetConfig ds2 = addDataset(namespace, splitsStore, "foo%bar", Arrays.asList("test", "foo%bar"), 20);
      final DatasetConfig ds3 = addDataset(namespace, splitsStore, UUID.randomUUID().toString(), Arrays.asList("test", "foobar"), 30);

      // Check that split ids are unescaped
      int count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.unsafeGetSplitsRange(ds1))) {
        DatasetSplitId splitId = entry.getKey();
        assertThat(splitId.getDatasetId(), is("foo"));
        assertThat(splitId.getSplitVersion(), is(Long.MIN_VALUE));
        count++;
      }
      assertThat(count, is(10));

      // Check that split ids are unescaped
      count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.unsafeGetSplitsRange(ds2))) {
        DatasetSplitId splitId = entry.getKey();
        assertThat(splitId.getDatasetId(), startsWith("foo")); // unescaped dataset split id might generate invalid unicode when unescaped
        assertThat(splitId.getSplitVersion(), is(42L));
        count++;
      }
      assertThat(count, is(20));

      // Check that split ids are unescaped
      count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.unsafeGetSplitsRange(ds3))) {
        DatasetSplitId splitId = entry.getKey();
        // dataset split id should be a valid uuid
        assertThat(splitId.getDatasetId(), is(ds3.getId().getId()));
        assertThat(splitId.getSplitVersion(), is(42L));
        count++;
      }
      assertThat(count, is(30));
      assertThat(StreamSupport.stream(splitsStore.find().spliterator(), false).count(), is(10L + 20L + 30L));

      // Perform upgrade
      final UpgradeContext context = new UpgradeContext(kvStoreProvider, null, null);
      final UpdateDatasetSplitIdTask task = new UpdateDatasetSplitIdTask();
      task.upgrade(context);

      // Verify new splits
      count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.getSplitsRange(ds1))) {
        DatasetSplitId splitId = entry.getKey();
        assertThat(splitId.getDatasetId(), is("foo_bar"));
        assertThat(splitId.getSplitVersion(), is(42L));
        count++;
      }
      assertThat(count, is(10));

      count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.getSplitsRange(ds2))) {
        DatasetSplitId splitId = entry.getKey();
        assertThat(splitId.getDatasetId(), startsWith("foo%bar")); // unescaped dataset split id might generate invalid unicode when unescaped
        assertThat(splitId.getSplitVersion(), is(42L));
        count++;
      }
      assertThat(count, is(20));

      count = 0;
      for(Map.Entry<DatasetSplitId, DatasetSplit> entry : splitsStore.find(DatasetSplitId.getSplitsRange(ds3))) {
        DatasetSplitId splitId = entry.getKey();
        assertThat(splitId.getDatasetId(), is(ds3.getId().getId()));
        assertThat(splitId.getSplitVersion(), is(42L));
        count++;
      }
      assertThat(count, is(30));


      assertThat(StreamSupport.stream(splitsStore.find().spliterator(), false).count(), is(10L + 20L + 30L));
    }


  }

  private DatasetConfig addDataset(IndexedStore<byte[], NameSpaceContainer> namespace, IndexedStore<DatasetSplitId, DatasetSplit> splitsStore,
      String id, List<String> path, int splits) {
    DatasetConfig ds = new DatasetConfig()
        .setId(new EntityId(id))
        .setName(last(path))
        .setFullPathList(path)
        .setType(DatasetType.PHYSICAL_DATASET)
        .setReadDefinition(new ReadDefinition().setSplitVersion(42L));

    namespace.put(
        NamespaceServiceImpl.getKey(new NamespaceKey(path)),
        new NameSpaceContainer().setType(NameSpaceContainer.Type.DATASET).setFullPathList(path).setDataset(ds));

    for(int i = 0; i < splits; i++) {
      final String splitKey = Integer.toString(i);
      DatasetSplit split = new DatasetSplit()
          .setSplitVersion(42L)
          .setSplitKey(splitKey);

      // Generate an older dataset split id
      DatasetSplitId splitId = UnsafeDatasetSplitIdHelper.of(ds, splitKey);
      splitsStore.put(splitId, split);

    }
    return ds;
  }

  private static <T> T last(List<T> list) {
    return list.get(list.size() - 1);
  }
}
