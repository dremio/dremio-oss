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

public class TestMaterializationCache {
  @Rule
  public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private MaterializationCache.CacheHelper provider;
  @Mock private NamespaceService namespaceService;
  @Mock private ReflectionStatusService reflectionStatusService;
  @Mock private CatalogService catalogService;

  /**
   * Test in case materialization expansion (deserialization) takes too long time
   * {@code MaterializationCache.update(Materialization m)} does not race with
   * {@code MaterializationCache.refresh()} and falls into infinite loop.
   */
  @Test (timeout = 10000)
  public void testMaterializationCacheUpdate() throws Exception {
    MaterializationCache materializationCache = spy(new MaterializationCache(provider, namespaceService, reflectionStatusService, catalogService));
    Materialization m1 = new Materialization();
    Materialization m2 = new Materialization();
    CachedMaterializationDescriptor descriptor = mock(CachedMaterializationDescriptor.class);
    MaterializationId mId1 = new MaterializationId("abc");
    MaterializationId mId2 = new MaterializationId("def");
    m1.setId(mId1);
    m2.setId(mId2);

    // For materializationCache.refresh()
    when(provider.expand(m1)).thenReturn(descriptor);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Arrays.asList());

    // For materializationCache.update(m2);
    when(provider.expand(m2)).thenAnswer(new Answer<CachedMaterializationDescriptor>() {
      @Override
      public CachedMaterializationDescriptor answer(InvocationOnMock invocation) throws InterruptedException {
        // Simulate MaterializationCache.update(Materialization m) takes long time during expansion
        Thread.sleep(3000);
        return descriptor;
      }
    });

    Runnable refreshTask = new Runnable() {
      @Override
      public void run() {
        while(true) {
          materializationCache.resetCache();
          materializationCache.refresh();
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    };

    Thread refreshThread = new Thread(refreshTask);
    refreshThread.start();

    materializationCache.update(m2);
    assertThat(materializationCache.get(mId2)).isEqualTo(descriptor);

    refreshThread.interrupt();
  }
}
