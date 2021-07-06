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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Unit tests for the NessieCommitKVStoreBuilder class.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestNessieRefKVStoreBuilder {
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
    new NessieRefKVStoreBuilder().build(storeBuildingFactory);

    verify(storeBuilder).name(NessieRefKVStoreBuilder.TABLE_NAME);

    verify(storeBuilder).keyFormat(any(Format.class));
    verify(storeBuilder).valueFormat(any(Format.class));

    verify(storeBuilder).build();
  }

  @Test
  public void testEncodeNameRefBranch() {
    assertEquals(NessieRefKVStoreBuilder.BRANCH_PREFIX + "my-branch", NessieRefKVStoreBuilder.encodeNamedRef(BranchName.of("my-branch")));
  }

  @Test
  public void testEncodeNameRefTag() {
    assertEquals(NessieRefKVStoreBuilder.TAG_PREFIX + "my-tag", NessieRefKVStoreBuilder.encodeNamedRef(TagName.of("my-tag")));
  }

  @Test
  public void testDecodeNameRefBranch() {
    NamedRef ref = NessieRefKVStoreBuilder.decodeNamedRef(NessieRefKVStoreBuilder.BRANCH_PREFIX + "my-branch");
    assertTrue(ref instanceof BranchName);
    assertEquals("my-branch", ref.getName());
  }

  @Test
  public void testDecodeNameRefTag() {
    NamedRef ref = NessieRefKVStoreBuilder.decodeNamedRef(NessieRefKVStoreBuilder.TAG_PREFIX + "my-tag");
    assertTrue(ref instanceof TagName);
    assertEquals("my-tag", ref.getName());
  }
}
