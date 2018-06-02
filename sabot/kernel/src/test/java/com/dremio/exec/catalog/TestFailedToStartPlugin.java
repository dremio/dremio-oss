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
package com.dremio.exec.catalog;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Provider;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.concurrent.Runnables;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.test.DremioTest;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 *
 */
public class TestFailedToStartPlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFailedToStartPlugin.class);

  private SabotContext sabotContext;
  private KVStoreProvider storeProvider;
  private SchedulerService schedulerService;
  private PluginsManager plugins;
  private static MockUpPlugin mockUpPlugin;

  private static final String MOCK_UP = "mockup-failed-to-start";

  @Before
  public void setup() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    storeProvider.start();
    final KVPersistentStoreProvider psp = new KVPersistentStoreProvider(
      new Provider<KVStoreProvider>() {
        @Override
        public KVStoreProvider get() {
          return storeProvider;
        }
      },
      true
    );
    final NamespaceService mockNamespaceService = mock(NamespaceService.class);
    mockUpPlugin = new MockUpPlugin();
    MetadataPolicy rapidRefreshPolicy = new MetadataPolicy()
      .setAuthTtlMs(1L)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setNamesRefreshMs(1L)
      .setDatasetDefinitionRefreshAfterMs(1L)
      .setDatasetDefinitionExpireAfterMs(1L);

    final SourceConfig mockUpConfig = new SourceConfig()
      .setName(MOCK_UP)
      .setMetadataPolicy(rapidRefreshPolicy)
      .setCtime(100L)
      .setConnectionConf(new MockUpConfig());

    when(mockNamespaceService.getSources())
      .thenReturn(Arrays.asList(mockUpConfig));
    when(mockNamespaceService.getSource(any(NamespaceKey.class)))
      .thenReturn(mockUpConfig);
    when(mockNamespaceService.getAllDatasets(any(NamespaceKey.class)))
      .thenReturn(Collections.emptyList());

    sabotContext = mock(SabotContext.class);
    // used in c'tor
    when(sabotContext.getClasspathScan())
      .thenReturn(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getNamespaceService(anyString()))
      .thenReturn(mockNamespaceService);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(SabotConfig.create(), CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence())
      .thenReturn(lpp);
    when(sabotContext.getStoreProvider())
      .thenReturn(psp);

    final SystemOptionManager som = new SystemOptionManager(CLASSPATH_SCAN_RESULT, lpp, psp);
    som.init();
    when(sabotContext.getOptionManager())
      .thenReturn(som);

    // used in start
    when(sabotContext.getKVStoreProvider())
      .thenReturn(storeProvider);
    when(sabotContext.getConfig())
      .thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);

    // used in newPlugin
    when(sabotContext.getRoles())
      .thenReturn(Sets.newHashSet(ClusterCoordinator.Role.MASTER));

    schedulerService = mock(SchedulerService.class);
    doAnswer(new Answer<Cancellable>() {
      @Override
      public Cancellable answer(InvocationOnMock invocation) {
        final Object[] arguments = invocation.getArguments();
        final Runnable r = (Runnable) arguments[1];
        Runnables.executeInSeparateThread(new Runnable() {
          @Override
          public void run() {
            // Next run scheduled for 1ms from now. Let's just sleep for that 1ms
            try {
              Thread.sleep(1L);
            } catch (Exception e) {
              // ignored
            }
            r.run();
          }

        });
        return mock(Cancellable.class);
      }
    }).when(schedulerService).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void testFailedToStart() throws Exception {
    KVStore<NamespaceKey, SourceInternalData> sourceDataStore = storeProvider.getStore(CatalogSourceDataCreator.class);
    plugins = new PluginsManager(sabotContext, sourceDataStore, schedulerService);

    mockUpPlugin.setThrowAtStart();
    assertEquals(0, mockUpPlugin.getNumFailedStarts());
    plugins.start();
    // mockUpPlugin should be failing over and over right around now
    Thread.sleep(10);
    long currNumFailedStarts = mockUpPlugin.getNumFailedStarts();
    assertTrue(currNumFailedStarts > 1);
    mockUpPlugin.unsetThrowAtStart();
    Thread.sleep(10);
    currNumFailedStarts = mockUpPlugin.getNumFailedStarts();
    Thread.sleep(10);
    assertEquals(currNumFailedStarts, mockUpPlugin.getNumFailedStarts());
  }

  @Test
  public void testBadSourceAtStart() throws Exception {
    KVStore<NamespaceKey, SourceInternalData> sourceDataStore = storeProvider.getStore(CatalogSourceDataCreator.class);
    plugins = new PluginsManager(sabotContext, sourceDataStore, schedulerService);

    mockUpPlugin.setSimulateBadState(true);
    assertEquals(false, mockUpPlugin.gotDatasets());
    plugins.start();
    // metadata refresh should be running right now
    Thread.sleep(10);
    assertFalse(mockUpPlugin.gotDatasets());
    mockUpPlugin.setSimulateBadState(false);
    // Give metadata refresh a chance to run again
    Thread.sleep(10);
    assertTrue(mockUpPlugin.gotDatasets());
    mockUpPlugin.unsetGotDatasets();
    Thread.sleep(10);
    assertTrue(mockUpPlugin.gotDatasets());
  }

  @SourceType(MOCK_UP)
  public static class MockUpConfig extends ConnectionConf<MockUpConfig, MockUpPlugin> {

    @Override
    public MockUpPlugin newPlugin(SabotContext context, String name,
                                  Provider<StoragePluginId> pluginIdProvider) {
      return mockUpPlugin;
    }
  }

  public static class MockUpPlugin implements StoragePlugin {
    private List<SourceTableDefinition> datasets;
    boolean throwAtStart = false;
    long numFailedStarts = 0;
    boolean gotDatasets = false;
    boolean simulateBadState = false;

    @Override
    public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) {
      gotDatasets = true;
      return Collections.emptyList();
    }

    @Override
    public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset,
                                            boolean ignoreAuthErrors) {
      return null;
    }

    @Override
    public boolean containerExists(NamespaceKey key) {
      return false;
    }

    @Override
    public boolean datasetExists(NamespaceKey key) {
      return false;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
    }

    @Override
    public SourceState getState() {
      if (throwAtStart) {
        return SourceState.badState("throwAtStart is set");
      }
      if (simulateBadState) {
        return SourceState.badState("simulated bad state");
      }
      return SourceState.goodState();
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
      return SourceCapabilities.NONE;
    }

    @Override
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return null;
    }

    @Override
    public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) {
      return CheckResult.UNCHANGED;
    }

    @Override
    public void close() {
    }

    @Override
    public void start() {
      if (throwAtStart) {
        ++numFailedStarts;
        throw UserException.resourceError().build(logger);
      }
    }

    public void setThrowAtStart() {
      throwAtStart = true;
    }
    public void unsetThrowAtStart() {
      throwAtStart = false;
    }

    public long getNumFailedStarts() {
      return numFailedStarts;
    }

    public void unsetGotDatasets() {
      gotDatasets = false;
    }
    public boolean gotDatasets() {
      return gotDatasets;
    }

    public void setSimulateBadState(boolean value) {
      simulateBadState = value;
    }
  }
}
