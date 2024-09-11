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

import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_RETRY_MINUTES;
import static com.dremio.service.reflection.proto.MaterializationState.DONE;
import static com.dremio.service.reflection.proto.MaterializationState.FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.acceleration.descriptor.ExpandedMaterializationDescriptor;
import com.dremio.exec.planner.serialization.DeserializationException;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.test.DremioTest;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

public class TestMaterializationCache extends DremioTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock private MaterializationCache.CacheHelper provider;
  @Mock private ReflectionStatusService reflectionStatusService;
  @Mock private CatalogService catalogService;
  @Mock private OptionManager optionManager;
  @Mock private Catalog catalog;
  @Mock private MaterializationStore materializationStore;
  @Mock private ExpandedMaterializationDescriptor descriptor;
  private Materialization m1;

  @Before
  public void setup() {
    when(catalogService.getCatalog(any())).thenReturn(catalog);
    m1 = new Materialization();
    m1.setReflectionId(new ReflectionId("r1"));
    m1.setState(DONE);
    m1.setReflectionId(new ReflectionId("r1"));
    m1.setId(new MaterializationId("abc"));
  }

  /**
   * Test in case materialization expansion (deserialization) takes too long time {@code
   * MaterializationCache.update(Materialization m)} does not race with {@code
   * MaterializationCache.refresh()} and fall into infinite loop. (test will timeout in such case)
   */
  @Test
  public void testMaterializationCacheUpdate() throws Exception {
    MaterializationCache materializationCache =
        spy(
            new MaterializationCache(
                provider,
                reflectionStatusService,
                catalogService,
                optionManager,
                materializationStore));
    Materialization m2 = new Materialization();
    m2.setReflectionId(new ReflectionId("r2"));
    MaterializationId mId2 = new MaterializationId("def");
    m2.setId(mId2);

    // For materializationCache.refresh()
    when(provider.expand(m1, catalog)).thenReturn(descriptor);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());
    materializationCache.refreshMaterializationCache();

    // For materializationCache.update(m2);
    when(provider.expand(m2, catalog))
        .thenAnswer(
            new Answer<ExpandedMaterializationDescriptor>() {
              @Override
              public ExpandedMaterializationDescriptor answer(InvocationOnMock invocation)
                  throws InterruptedException {
                // Simulate MaterializationCache.update(Materialization m) takes long time during
                // expansion
                // and during this time the cache entry has been refreshed. Before DX-54194's fix
                // this will
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

  @Test
  public void testRetrySuccessful() throws Exception {
    MaterializationCache materializationCache =
        new MaterializationCache(
            provider, reflectionStatusService, catalogService, optionManager, materializationStore);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());
    when(optionManager.getOption(MATERIALIZATION_CACHE_RETRY_MINUTES)).thenReturn(60L);
    when(optionManager.getOption(MATERIALIZATION_CACHE_ENABLED)).thenReturn(true);

    // First sync will fail
    when(provider.expand(m1, catalog))
        .thenThrow(new DeserializationException("Something not catalog related"));
    assertThat(materializationCache.getRetryMap().getIfPresent(m1.getId())).isNull();
    materializationCache.refreshMaterializationCache();
    assertThat(m1.getState()).isEqualTo(DONE);
    assertThat(materializationCache.getRetryMap().getIfPresent(m1.getId())).isNotNull();

    // Second sync will succeed
    reset(provider);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());
    when(provider.expand(m1, catalog)).thenReturn(descriptor);
    materializationCache.refreshMaterializationCache();
    assertThat(materializationCache.getRetryMap().getIfPresent(m1.getId())).isNull();
  }

  @Test
  public void testRetryFailed() throws Exception {
    MaterializationCache materializationCache =
        new MaterializationCache(
            provider, reflectionStatusService, catalogService, optionManager, materializationStore);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());
    when(materializationStore.get(m1.getId())).thenReturn(m1);

    // Setup the map such that the m1 has been retrying for 60 minutes
    materializationCache
        .getRetryMap()
        .put(
            m1.getId(),
            System.currentTimeMillis()
                - Duration.ofMinutes(optionManager.getOption(MATERIALIZATION_CACHE_RETRY_MINUTES))
                    .toMillis());

    // A failure that is not source down will result in given up
    when(provider.expand(m1, catalog)).thenThrow(new DeserializationException("Planner bomb!"));
    materializationCache.refreshMaterializationCache();
    assertThat(m1.getState()).isEqualTo(FAILED);
    assertThat(m1.getFailure().getMessage())
        .isEqualTo(
            "Materialization Cache Failure: Error expanding materialization r1/abc. All retries exhausted. Updated to FAILED. Planner bomb!");
    assertThat(materializationCache.getRetryMap().getIfPresent(m1.getId())).isNull();
  }

  @Test
  public void testRetryUnlimitedForSourceDown() throws Exception {
    MaterializationCache materializationCache =
        new MaterializationCache(
            provider, reflectionStatusService, catalogService, optionManager, materializationStore);
    when(provider.getValidMaterializations()).thenReturn(Arrays.asList(m1));
    when(provider.getExternalReflections()).thenReturn(Collections.emptyList());
    when(optionManager.getOption(MATERIALIZATION_CACHE_RETRY_MINUTES)).thenReturn(60L);

    // Setup the map such that the m1 has been retrying for 60 minutes
    materializationCache
        .getRetryMap()
        .put(
            m1.getId(),
            System.currentTimeMillis()
                - Duration.ofMinutes(optionManager.getOption(MATERIALIZATION_CACHE_RETRY_MINUTES))
                    .toMillis());

    // A failure that is source down doesn't result in giving up retries
    when(provider.expand(m1, catalog)).thenThrow(UserException.sourceInBadState().buildSilently());
    materializationCache.refreshMaterializationCache();
    assertThat(m1.getState()).isEqualTo(DONE);
    assertThat(materializationCache.getRetryMap().getIfPresent(m1.getId())).isNotNull();
  }
}
