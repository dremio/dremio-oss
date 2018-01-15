/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator;

import static com.dremio.exec.ExecConstants.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.exec.ExecConstants.MATERIALIZATION_CACHE_REFRESH_DURATION;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.IncrementalUpdateSettings;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.MaterializationDescriptor.LayoutInfo;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.StoragePluginRegistry;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
/**
 * Unit tests CacheMaterializationProvider
 */
public class TestCachedMaterializationProvider {
  @Mock
  private MaterializationDescriptorProvider provider;
  @Mock
  private Provider<SabotContext> contextProvider;
  @Mock
  private SabotContext context;
  @Mock
  private StoragePluginRegistry storageRegistry;
  @Mock
  private SystemOptionManager options;
  @Mock
  private AccelerationService accel;
  @Mock
  private FunctionImplementationRegistry registry;
  @Mock
  private BufferAllocator allocator;
  @Mock
  private OptionValue value;
  @Mock
  private SabotConfig config;
  @Mock
  private MaterializationDescriptor materialization1;
  @Mock
  private MaterializationDescriptor materialization2;
  @Mock
  private MaterializationDescriptor materialization3;
  @Mock
  private DremioRelOptMaterialization relOptMat1;
  @Mock
  private DremioRelOptMaterialization relOptMat2;
  @Mock
  private DremioRelOptMaterialization relOptMat3;
  @Mock
  private LayoutInfo layout1;
  @Mock
  private LayoutInfo layout2;
  @Mock
  private IncrementalUpdateSettings settings;
  @Mock
  private SqlConverter converter;
  @Mock
  private QueryContext queryContext;
  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testAddMaterialization() throws Exception {
    when(contextProvider.get()).thenReturn(context);
    when(context.getStorage()).thenReturn(storageRegistry);
    when(context.getFunctionImplementationRegistry()).thenReturn(registry);
    when(context.getOptionManager()).thenReturn(options);
    when(options.getOption(PlannerSettings.PLANNER_MEMORY_LIMIT.getOptionName())).thenReturn(OptionValue.createLong(OptionType.SYSTEM, "name", 128L));
    when(context.getAllocator()).thenReturn(allocator);
    when(context.getConfig()).thenReturn(config);
    when(options.getOption(MATERIALIZATION_CACHE_REFRESH_DURATION.getOptionName())).thenReturn(OptionValue.createLong(OptionType.SYSTEM, "name", 30L));
    when(options.getOption(MATERIALIZATION_CACHE_ENABLED.getOptionName())).thenReturn(OptionValue.createBoolean(OptionType.SYSTEM, "name", true));


    when(materialization1.getMaterializationFor(converter)).thenReturn(relOptMat1);
    when(materialization1.getAccelerationId()).thenReturn("rid-1");
    when(materialization1.getMaterializationId()).thenReturn("mid-1");
    when(materialization1.getLayoutInfo()).thenReturn(layout1);
    when(materialization1.getPlan()).thenReturn(new byte[0]);
    when(materialization1.getPath()).thenReturn(ImmutableList.of("test1"));
    when(materialization1.getIncrementalUpdateSettings()).thenReturn(settings);
    when(materialization1.isComplete()).thenReturn(true);
    when(layout1.getLayoutId()).thenReturn("layout1");

    when(provider.get(false)).thenReturn(ImmutableList.of(materialization1));
    when(provider.get(true)).thenReturn(ImmutableList.of(materialization1));


    CachedMaterializationProvider materializationProvider =
        new CachedMaterializationProvider(provider, contextProvider, accel);
    materializationProvider.setDebug(true);

    materializationProvider.setDefaultConverter(converter);

    assertEquals(ImmutableList.of("mid-1"),
        getMaterializationIds(materializationProvider));

    when(materialization2.getMaterializationFor(converter)).thenReturn(relOptMat2);
    when(materialization2.getAccelerationId()).thenReturn("rid-2");
    when(materialization2.getExpirationTimestamp()).thenReturn(3L);
    when(materialization2.getMaterializationId()).thenReturn("mid-2");
    when(materialization2.getLayoutInfo()).thenReturn(layout2);
    when(materialization2.getPlan()).thenReturn(new byte[0]);
    when(materialization2.getPath()).thenReturn(ImmutableList.of("test2"));
    when(materialization2.getIncrementalUpdateSettings()).thenReturn(settings);
    when(materialization2.isComplete()).thenReturn(true);
    when(layout2.getLayoutId()).thenReturn("layout2");


    when(provider.get(false)).thenReturn(ImmutableList.of(materialization1, materialization2));
    when(provider.get(true)).thenReturn(ImmutableList.of(materialization1, materialization2));
    materializationProvider.update(materialization2);

    assertEquals(ImmutableList.of("mid-1", "mid-2"),
        getMaterializationIds(materializationProvider));

    when(materialization3.getMaterializationFor(converter)).thenReturn(relOptMat3);
    when(materialization3.getAccelerationId()).thenReturn("rid-2");
    when(materialization3.getExpirationTimestamp()).thenReturn(3L);
    when(materialization3.getMaterializationId()).thenReturn("mid-3");
    when(materialization3.getLayoutInfo()).thenReturn(layout2);
    when(materialization3.getPlan()).thenReturn(new byte[0]);
    when(materialization3.getPath()).thenReturn(ImmutableList.of("test2"));
    when(materialization3.getIncrementalUpdateSettings()).thenReturn(settings);
    when(materialization3.isComplete()).thenReturn(true);

    when(provider.get(false)).thenReturn(ImmutableList.of(materialization1, materialization3));
    when(provider.get(true)).thenReturn(ImmutableList.of(materialization1, materialization3));
    materializationProvider.update(materialization3);

    assertEquals(ImmutableList.of("mid-1", "mid-3"),
        getMaterializationIds(materializationProvider));

    when(materialization2.isComplete()).thenReturn(false);


    when(provider.get(false)).thenReturn(ImmutableList.of(materialization1, materialization2, materialization3));
    when(provider.get(true)).thenReturn(ImmutableList.of(materialization1, materialization2, materialization3));
    materializationProvider.update(materialization3);

    assertEquals(ImmutableList.of("mid-1", "mid-3"),
        getMaterializationIds(materializationProvider));

    assertEquals(ImmutableList.of("mid-1", "mid-3"),
        getMaterializationIds(materializationProvider, false));

    assertEquals(ImmutableList.of("mid-1", "mid-2", "mid-3"),
        getMaterializationIds(materializationProvider, true));

      materializationProvider.close();
  }

  @Ignore("DX-9565")
  @Test
  public void testRemoveMaterialization() throws Exception {
    when(contextProvider.get()).thenReturn(context);
    when(context.getStorage()).thenReturn(storageRegistry);
    when(context.getFunctionImplementationRegistry()).thenReturn(registry);
    when(context.getOptionManager()).thenReturn(options);
    when(options.getOption(PlannerSettings.PLANNER_MEMORY_LIMIT.getOptionName())).thenReturn(OptionValue.createLong(OptionType.SYSTEM, "name", 128L));
    when(context.getAllocator()).thenReturn(allocator);
    when(context.getConfig()).thenReturn(config);
    when(options.getOption(MATERIALIZATION_CACHE_REFRESH_DURATION.getOptionName())).thenReturn(OptionValue.createLong(OptionType.SYSTEM, "name", 30L));
    when(options.getOption(MATERIALIZATION_CACHE_ENABLED.getOptionName())).thenReturn(OptionValue.createBoolean(OptionType.SYSTEM, "name", true));

    when(materialization1.getMaterializationFor(converter)).thenReturn(relOptMat1);
    when(materialization1.getAccelerationId()).thenReturn("rid-1");
    when(materialization1.getMaterializationId()).thenReturn("mid-1");
    when(materialization1.getLayoutInfo()).thenReturn(layout1);
    when(materialization1.getPlan()).thenReturn(new byte[0]);
    when(materialization1.getPath()).thenReturn(ImmutableList.of("test1"));
    when(materialization1.getIncrementalUpdateSettings()).thenReturn(settings);
    when(materialization1.isComplete()).thenReturn(true);
    when(layout1.getLayoutId()).thenReturn("layout1");

    when(materialization2.getMaterializationFor(converter)).thenReturn(relOptMat2);
    when(materialization2.getAccelerationId()).thenReturn("rid-2");
    when(materialization2.getExpirationTimestamp()).thenReturn(3L);
    when(materialization2.getMaterializationId()).thenReturn("mid-2");
    when(materialization2.getLayoutInfo()).thenReturn(layout2);
    when(materialization2.getPlan()).thenReturn(new byte[0]);
    when(materialization2.getPath()).thenReturn(ImmutableList.of("test2"));
    when(materialization2.getIncrementalUpdateSettings()).thenReturn(settings);
    when(materialization2.isComplete()).thenReturn(true);
    when(layout2.getLayoutId()).thenReturn("layout2");

    when(provider.get(Mockito.anyBoolean())).thenReturn(ImmutableList.of(materialization1, materialization2));

    CachedMaterializationProvider materializationProvider =
        new CachedMaterializationProvider(provider, contextProvider, accel);
    materializationProvider.setDebug(true);

    materializationProvider.setDefaultConverter(converter);

    assertEquals(ImmutableList.of("mid-1", "mid-2"),
        getMaterializationIds(materializationProvider));

    materializationProvider.setDebug(false);

    materializationProvider.remove("mid-1");

    assertEquals(ImmutableList.of("mid-2"),
        getMaterializationIds(materializationProvider));

    materializationProvider.remove("mid-2");

    assertEquals(ImmutableList.of(),
        getMaterializationIds(materializationProvider));

      materializationProvider.close();

  }

  private List<String> getMaterializationIds(CachedMaterializationProvider materializationProvider, boolean includeInComplete) {
    List<String> materailizationIds = FluentIterable.from(
        materializationProvider.get(includeInComplete)).transform(new Function<MaterializationDescriptor, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final MaterializationDescriptor input) {
            return input.getMaterializationId();
          }
        })
        .toList();
    return materailizationIds;
  }

  private List<String> getMaterializationIds(CachedMaterializationProvider materializationProvider) {
    List<String> materailizationIds = FluentIterable.from(
        materializationProvider.get()).transform(new Function<MaterializationDescriptor, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final MaterializationDescriptor input) {
            return input.getMaterializationId();
          }
        })
        .toList();
    return materailizationIds;  }
}
