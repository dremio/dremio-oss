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

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.test.DremioTest;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCachingNamespaceStore extends DremioTest {
  private LocalKVStoreProvider kvStoreProvider;
  private IndexedStore<String, NameSpaceContainer> underlyingStore;
  private CachingNamespaceStore cachingNamespaceStore;

  @Before
  public void setup() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    underlyingStore = kvStoreProvider.getStore(NamespaceStore.NamespaceStoreCreator.class);
    cachingNamespaceStore = new CachingNamespaceStore(underlyingStore);
  }

  @After
  public void shutdown() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void testBasicPut() {
    NameSpaceContainer container = new NameSpaceContainer();
    String name = "space1";
    container.setSpace(
        new SpaceConfig().setName(name).setId(new EntityId(UUID.randomUUID().toString())));
    container.setType(NameSpaceContainer.Type.SPACE);
    container.setFullPathList(List.of(name));

    String key = new NamespaceInternalKey(new NamespaceKey(name)).getKey();
    Document<String, NameSpaceContainer> putResult = cachingNamespaceStore.put(key, container);

    Assert.assertEquals(key, putResult.getKey());
    Assert.assertEquals(container, putResult.getValue());
    Assert.assertNotNull(putResult.getTag());

    // Replace the container with a completely different one in the underlying kv store, bypassing
    // the cache
    NameSpaceContainer container2 = new NameSpaceContainer();
    container2.setSource(
        new SourceConfig().setName(name).setId(new EntityId(UUID.randomUUID().toString())));
    container2.setType(NameSpaceContainer.Type.SOURCE);
    container2.setFullPathList(List.of(name));
    underlyingStore.put(key, container2);

    Document<String, NameSpaceContainer> getResult = cachingNamespaceStore.get(key);

    // Make sure we got the original container (cache hit) and don't reach out to the underlying kv
    // store
    Assert.assertEquals(key, getResult.getKey());
    Assert.assertEquals(container, getResult.getValue());
    Assert.assertEquals(putResult.getTag(), getResult.getTag());

    cachingNamespaceStore.invalidateCache(key);

    // Now we should get the new one
    Document<String, NameSpaceContainer> getResult2 = cachingNamespaceStore.get(key);

    Assert.assertEquals(key, getResult2.getKey());
    Assert.assertEquals(container2, getResult2.getValue());
    Assert.assertNotNull(getResult2.getTag());
    Assert.assertNotEquals(getResult.getTag(), getResult2.getTag());
  }
}
