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
package com.dremio.datastore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.options.ImmutableMaxResultsOption;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestLocalDataStoreRpcHandler {
  @Mock private CoreStoreProviderRpcService rpcService;

  private LocalDataStoreRpcHandler rpcHandler;

  @BeforeEach
  public void setUp() {
    rpcHandler = new LocalDataStoreRpcHandler("host", rpcService);
  }

  @Test
  public void testFind_maxResults() {
    String storeId = "store";
    CoreKVStore<Object, Object> kvStore = mock(CoreKVStore.class);
    when(rpcService.getStore(eq(storeId))).thenReturn(kvStore);
    when(kvStore.find(any())).thenReturn(ImmutableList.of());

    int maxResults = 10;
    rpcHandler.find(
        RemoteDataStoreProtobuf.FindRequest.newBuilder()
            .setStoreId(storeId)
            .setMaxResults(maxResults)
            .build());

    verify(kvStore, times(1))
        .find(
            eq(
                new KVStore.FindOption[] {
                  new ImmutableMaxResultsOption.Builder().setMaxResults(maxResults).build()
                }));
  }
}
