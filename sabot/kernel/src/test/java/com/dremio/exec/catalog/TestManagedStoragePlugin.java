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
package com.dremio.exec.catalog;

import static com.dremio.exec.catalog.conf.ConnectionConf.USE_EXISTING_SECRET_VALUE;
import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.exceptions.UserException;
import com.dremio.concurrent.Runnables;
import com.dremio.concurrent.SafeRunnable;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.server.options.SystemOptionManagerImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.test.DremioTest;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for ManagedStoragePlugin, for the subset of functionality not covered in
 * TestPluginsManager.
 */
public class TestManagedStoragePlugin {
  private LegacyKVStoreProvider storeProvider;
  private NamespaceService mockNamespaceService;
  private SabotContext sabotContext;
  private OptionManager optionManager;
  private CredentialsService revealSecretService;
  private SecretsCreator secretsCreator;
  private SchedulerService schedulerService;
  private MetadataRefreshInfoBroadcaster broadcaster;
  private ModifiableSchedulerService modifiableSchedulerService;

  private final CloseableThreadPool executor =
      new CloseableThreadPool("test-managed-storage-plugin");
  private final List<Cancellable> scheduledTasks = new ArrayList<>();

  private static final String INSPECTOR = "inspector";

  @Before
  public void setup() throws Exception {
    storeProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    storeProvider.start();
    mockNamespaceService = mock(NamespaceService.class);
    when(mockNamespaceService.getAllDatasets(Mockito.any())).thenReturn(Collections.emptyList());

    // Possibly used in constructor
    final DatasetListingService mockDatasetListingService = mock(DatasetListingService.class);
    sabotContext = mock(SabotContext.class);
    when(sabotContext.getClasspathScan()).thenReturn(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(mockNamespaceService);
    when(sabotContext.getDatasetListing()).thenReturn(mockDatasetListingService);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence()).thenReturn(lpp);

    final OptionValidatorListing optionValidatorListing =
        new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    final SystemOptionManager som =
        new SystemOptionManagerImpl(optionValidatorListing, lpp, () -> storeProvider, true);
    optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(new DefaultOptionManager(optionValidatorListing))
            .withOptionManager(som)
            .build();
    som.start();
    when(sabotContext.getOptionManager()).thenReturn(optionManager);

    when(sabotContext.getCredentialsServiceProvider()).thenReturn(() -> revealSecretService);
    when(sabotContext.getSecretsCreator()).thenReturn(() -> secretsCreator);

    // Set up a CredentialsService to always return the secret.
    // This is to verify the secret string stored in the SecretRef is a plain-text or not.
    revealSecretService = mock(CredentialsService.class);
    when(revealSecretService.lookup(anyString()))
        .thenAnswer((Answer<String>) invocation -> invocation.getArgument(0));
    when(revealSecretService.isSupported(any()))
        .thenAnswer(input -> input.getArgument(0).toString().startsWith("secret:///"));

    // Configure the secrets services to return a URI for each incoming secret
    // and to recognize a prefix when determining whether something is encrypted.
    secretsCreator = mock(SecretsCreator.class);
    when(secretsCreator.encrypt(any()))
        .thenAnswer(input -> Optional.of(new URI("secret:///" + input.getArgument(0))));
    when(secretsCreator.isEncrypted(startsWith("secret:///"))).thenReturn(true);
    when(secretsCreator.cleanup(any())).thenReturn(true);

    schedulerService = mock(SchedulerService.class);
    mockScheduleInvocation();
    broadcaster = mock(MetadataRefreshInfoBroadcaster.class);
    doNothing().when(broadcaster).communicateChange(any());

    PositiveLongValidator option = ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES;
    modifiableSchedulerService =
        new ModifiableLocalSchedulerService(
            1, "modifiable-scheduler-", option, () -> optionManager) {
          @Override
          public Cancellable schedule(Schedule schedule, Runnable task) {
            Cancellable wakeupTask = super.schedule(schedule, task);
            scheduledTasks.add(wakeupTask);
            return wakeupTask;
          }
        };
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(modifiableSchedulerService, storeProvider);
  }

  private void mockScheduleInvocation() {
    // using SafeRunnable, as Runnable is also used to run initial setup that used to
    // run w/o any scheduling
    doAnswer(
            (Answer<Cancellable>)
                invocation -> {
                  final Object[] arguments = invocation.getArguments();
                  if (arguments[1] instanceof SafeRunnable) {
                    return mock(Cancellable.class);
                  }
                  // allow thread that does first piece of work: scheduleMetadataRefresh
                  // (that was not part of thread before) go through
                  final Runnable r = (Runnable) arguments[1];
                  Runnables.executeInSeparateThread(r);
                  return mock(Cancellable.class);
                })
        .when(schedulerService)
        .schedule(any(Schedule.class), any(Runnable.class));
  }

  private ManagedStoragePlugin newPlugin(SourceConfig config) {
    final Orphanage mockOrphanage = mock(Orphanage.class);
    final LegacyKVStore<NamespaceKey, SourceInternalData> sourceDataStore =
        storeProvider.getStore(CatalogSourceDataCreator.class);

    return new ManagedStoragePlugin(
        sabotContext,
        executor,
        true,
        modifiableSchedulerService,
        mockNamespaceService,
        mockOrphanage,
        sourceDataStore,
        config,
        optionManager,
        new ConnectionReaderDecorator(
            ConnectionReader.of(sabotContext.getClasspathScan(), ConnectionReaderImpl.class),
            () -> revealSecretService),
        CatalogServiceMonitor.DEFAULT.forPlugin(config.getName()),
        () -> broadcaster,
        null);
  }

  /**
   * Note: This is a partial copy of the test by the same name in TestPluginsManager, since they
   * test the same code, but at a slightly different layer.
   */
  @Test
  public void testCreateSourceFailedWithSecrets() throws Exception {
    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false).setSecret1("some-secret").toBytesString());

    final ManagedStoragePlugin plugin = newPlugin(newConfig);

    doThrow(
            UserException.validationError()
                .message("Failed to create for some reason")
                .buildSilently())
        .when(mockNamespaceService)
        .addOrUpdateSource(newConfig.getKey(), newConfig);
    // scheduledTasks.clear();

    assertThrows(UserException.class, () -> plugin.createSource(newConfig, "testuser"));
    // TODO: Figure out if this being empty is a problem with the test or not.
    // assertEquals(scheduledTasks.size(), 1);
    // assertTrue(scheduledTasks.get(0).isCancelled());

    // Check that the secret was encrypted (happens before failure) and deleted
    verify(secretsCreator, times(1)).encrypt(eq("some-secret"));
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///some-secret")));
  }

  @Test
  public void testUpdateSourceSuccessWithNewSecrets() throws Exception {
    final SourceConfig oldConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("secret:///some-old-secret")
                    .toBytesString());
    when(mockNamespaceService.getSource(eq(oldConfig.getKey()))).thenReturn(oldConfig);
    final ManagedStoragePlugin plugin = newPlugin(oldConfig);

    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("some-new-secret")
                    .setSecret2("another-new-secret")
                    .toBytesString());

    plugin.updateSource(newConfig, "testuser");

    // Check that the new secrets were encrypted and the old one deleted
    verify(secretsCreator, times(1)).encrypt(eq("some-new-secret"));
    verify(secretsCreator, times(1)).encrypt(eq("another-new-secret"));
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///some-old-secret")));
  }

  @Test
  public void testUpdateSourceSuccessWithExistingSecret() throws Exception {
    final SourceConfig oldConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("secret:///some-old-secret")
                    .toBytesString());
    when(mockNamespaceService.getSource(eq(oldConfig.getKey()))).thenReturn(oldConfig);
    final ManagedStoragePlugin plugin = newPlugin(oldConfig);

    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1(USE_EXISTING_SECRET_VALUE)
                    .setSecret3("another-new-secret")
                    .toBytesString());

    plugin.updateSource(newConfig, "testuser");

    // Check that the new secret was encrypted and others are unchanged
    verify(secretsCreator, times(1)).encrypt(eq("another-new-secret"));
    verify(secretsCreator, times(0)).cleanup(any());
  }

  @Test
  public void testUpdateSourceFailureWithNewSecret() throws Exception {
    final SourceConfig oldConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("secret:///some-old-secret")
                    .toBytesString());
    when(mockNamespaceService.getSource(eq(oldConfig.getKey()))).thenReturn(oldConfig);
    final ManagedStoragePlugin plugin = newPlugin(oldConfig);

    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("some-new-secret")
                    .setSecret2("another-new-secret")
                    .toBytesString());

    doThrow(
            UserException.validationError()
                .message("Failed to update for some reason")
                .buildSilently())
        .when(mockNamespaceService)
        .addOrUpdateSource(newConfig.getKey(), newConfig);

    assertThrows(UserException.class, () -> plugin.updateSource(newConfig, "testuser"));

    // Check that the new secrets were encrypted and then deleted due to update failure
    verify(secretsCreator, times(1)).encrypt(eq("some-new-secret"));
    verify(secretsCreator, times(1)).encrypt(eq("another-new-secret"));
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///some-new-secret")));
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///another-new-secret")));
  }

  @Test
  public void testUpdateSourceFailureWithExistingSecret() throws Exception {
    final SourceConfig oldConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("secret:///some-old-secret")
                    .toBytesString());
    when(mockNamespaceService.getSource(eq(oldConfig.getKey()))).thenReturn(oldConfig);
    final ManagedStoragePlugin plugin = newPlugin(oldConfig);

    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1(USE_EXISTING_SECRET_VALUE)
                    .setSecret2("another-new-secret")
                    .toBytesString());

    doThrow(
            UserException.validationError()
                .message("Failed to update for some reason")
                .buildSilently())
        .when(mockNamespaceService)
        .addOrUpdateSource(newConfig.getKey(), newConfig);

    assertThrows(UserException.class, () -> plugin.updateSource(newConfig, "testuser"));

    // Check that the new secrets were encrypted and then deleted due to update failure
    verify(secretsCreator, times(1)).encrypt(eq("another-new-secret"));
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///another-new-secret")));
  }

  @Test
  public void testDeleteSourceWithSecrets() throws Exception {
    final SourceConfig newConfig =
        new SourceConfig()
            .setType(INSPECTOR)
            .setName("TEST")
            .setTag("some-tag")
            .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
            .setConfig(
                new TestPluginsManager.Inspector(false)
                    .setSecret1("secret:///some-new-secret")
                    .toBytesString());

    final ManagedStoragePlugin plugin = newPlugin(newConfig);

    plugin.close(newConfig, c -> {});

    // Check that the existing secrets were deleted
    verify(secretsCreator, times(0)).encrypt(any());
    verify(secretsCreator, times(1)).cleanup(eq(URI.create("secret:///some-new-secret")));
  }
}
