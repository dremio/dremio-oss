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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.ConcurrentModificationException;
import org.junit.jupiter.api.Test;

public class TestNamespaceStore {
  @Test
  public void testPut_propagatesConcurrentModificationException() {
    IndexedStore<String, NameSpaceContainer> kvStore = mock(IndexedStore.class);
    NamespaceStore namespaceStore = new NamespaceStore(kvStore);

    String key = "key";
    NameSpaceContainer value =
        new NameSpaceContainer().setSpace(new SpaceConfig()).setType(NameSpaceContainer.Type.SPACE);
    when(kvStore.put(eq(key), eq(value), any()))
        .thenThrow(new ConcurrentModificationException("test"));

    ConcurrentModificationException e =
        assertThrows(ConcurrentModificationException.class, () -> namespaceStore.put(key, value));
    assertThat(e.getMessage()).isEqualTo("test");
  }
}
