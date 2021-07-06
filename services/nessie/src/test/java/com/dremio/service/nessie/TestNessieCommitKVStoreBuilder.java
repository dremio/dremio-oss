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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableDelete;
import org.projectnessie.versioned.Key;

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Unit tests for the NessieCommitKVStoreBuilder class.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestNessieCommitKVStoreBuilder {
  @Mock private StoreBuildingFactory storeBuildingFactory;
  @Mock private KVStoreProvider.StoreBuilder storeBuilder;

  @Before
  public void setup() {
    when(storeBuildingFactory.newStore()).thenReturn(storeBuilder);
    when(storeBuilder.name(anyString())).thenReturn(storeBuilder);
    when(storeBuilder.keyFormat(any(Format.class))).thenReturn(storeBuilder);
    when(storeBuilder.valueFormat(any(Format.class))).thenReturn(storeBuilder);
  }

  @Test
  public void testBuild() {
    new NessieCommitKVStoreBuilder().build(storeBuildingFactory);

    verify(storeBuilder).name(NessieCommitKVStoreBuilder.TABLE_NAME);

    verify(storeBuilder).keyFormat(any(Format.class));
    verify(storeBuilder).valueFormat(any(Format.class));

    verify(storeBuilder).build();
  }

  @Test
  public void testConvertCommit() {
    final NessieCommit original = new NessieCommit(
      Hash.of("0011223344556677"),
      Hash.of("7766554433221100"),
      ImmutableCommitMeta
        .builder()
        .commiter("Foo Bar")
        .email("foo@bar.com")
        .message("blah")
        .commitTime(System.currentTimeMillis())
        .build(),
      Collections.singletonList(ImmutableDelete.<Contents>builder().key(Key.of("a", "b")).shouldMatchHash(true).build())
    );

    final Commit converted = NessieCommitKVStoreBuilder.stringToCommit(NessieCommitKVStoreBuilder.commitToString(original));

    assertEquals(original.getHash(), converted.getHash());
    assertEquals(original.getAncestor(), converted.getAncestor());
    assertEquals(original.getMetadata(), converted.getMetadata());
    assertEquals(original.getOperations(), converted.getOperations());
  }
}
