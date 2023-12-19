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
package com.dremio.service.reflection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

import com.dremio.exec.planner.acceleration.CachedMaterializationDescriptor;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.test.DremioTest;

public class TestMaterializationCache extends DremioTest {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private MaterializationCache.CacheHelper provider;
  @Mock private NamespaceService namespaceService;
  @Mock private ReflectionStatusService reflectionStatusService;
  @Mock private CatalogService catalogService;

  /**
   * Test in case materialization expansion (deserialization) takes too long time
   * {@code MaterializationCache.update(Materialization m)} does not race with
   * {@code MaterializationCache.refresh()} and fall into infinite loop.
   * (test will timeout in such case)
   */
  @Test
  public void testMaterializationCacheUpdate() throws Exception {
    MaterializationCache materializationCache = spy(new MaterializationCache(provider, reflectionStatusService, catalogService));
    Materialization m1 = new Materialization();
    m1.setReflectionId(new ReflectionId("r1"));
    Materialization m2 = new Materialization();
    m2.setReflectionId(new ReflectionId("r2"));
    CachedMaterializationDescriptor descriptor = mock(CachedMaterializationDescriptor.class);
    MaterializationId mId1 = new MaterializationId("abc");
    MaterializationId mId2 = new MaterializationId("def");
    m1.setId(mId1);
    m2.setId(mId2);

    // For materializationCache.refresh()
    when(provider.expand(m1)).thenReturn(descriptor);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());

    // For materializationCache.update(m2);
    when(provider.expand(m2)).thenAnswer(new Answer<CachedMaterializationDescriptor>() {
      @Override
      public CachedMaterializationDescriptor answer(InvocationOnMock invocation) throws InterruptedException {
        // Simulate MaterializationCache.update(Materialization m) takes long time during expansion
        // and during this time the cache entry has been refreshed. Before DX-54194's fix this will
        // cause MaterializationCache.update(Materialization m) runs into infinite loop.
        materializationCache.resetCache();
        materializationCache.refreshMaterializationCache();
        // The sleep here is to avoid exhausting CPU time in case infinite loop happens.
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
        return descriptor;
      }
    });

    materializationCache.update(m2);
    assertThat(materializationCache.get(mId2)).isEqualTo(descriptor);
  }
}
