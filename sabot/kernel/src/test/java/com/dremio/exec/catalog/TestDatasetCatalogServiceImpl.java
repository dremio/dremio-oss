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

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.service.DirectProvider;
import com.dremio.service.catalog.AddOrUpdateDatasetRequest;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.OperationType;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceInvalidStateException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetCommonProtobuf;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.file.proto.FileProtobuf;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Sets;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

/**
 * Unit tests for {@link DatasetCatalogServiceImpl}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestDatasetCatalogServiceImpl.EndToEndTests.class, TestDatasetCatalogServiceImpl.StatusCodeTranslationTests.class})
public class TestDatasetCatalogServiceImpl {
  private static class CapturingStreamObserver<E> implements StreamObserver<E> {
    private E value;
    private Throwable exception;
    private boolean completed = false;

    @Override
    public void onNext(E e) {
      this.value = e;
    }

    @Override
    public void onError(Throwable throwable) {
      exception = throwable;
      completed = true;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }
  }

  public static class EndToEndTests {
    private static final String HOSTNAME = "localhost";
    private static final int THREAD_COUNT = 2;
    private static final long RESERVATION = 0;
    private static final long MAX_ALLOCATION = Long.MAX_VALUE;
    private static final int TIMEOUT = 0;
    private static final String NEW_DATASET = "newDataset";
    private static final NamespaceKey MOCKUP_NEWDATASET = new NamespaceKey(Arrays.asList(TestCatalogServiceImpl.MOCK_UP, NEW_DATASET));

    private static TestCatalogServiceImpl.MockUpPlugin mockUpPlugin;

    private LegacyKVStoreProvider storeProvider;
    private KVStoreProvider kvStoreProvider;
    private NamespaceService namespaceService;
    private Orphanage orphanage;
    private DatasetListingService datasetListingService;
    private BufferAllocator allocator;
    private LocalClusterCoordinator clusterCoordinator;
    private CloseableThreadPool pool;
    private FabricService fabricService;
    private NamespaceKey mockUpKey;
    private CatalogServiceImpl catalogService;
    private DatasetCatalogServiceImpl datasetCatalogService;

    @Rule
    public TemporarySystemProperties properties = new TemporarySystemProperties();

    @Before
    public void setup() throws Exception {
      properties.set("dremio_masterless", "false");
      final SabotConfig sabotConfig = SabotConfig.create();
      final DremioConfig dremioConfig = DremioConfig.create();

      final SabotContext sabotContext = mock(SabotContext.class);

      storeProvider =
        LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
      storeProvider.start();
      namespaceService = new NamespaceServiceImpl(storeProvider);

      kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
      kvStoreProvider.start();
      orphanage = new OrphanageImpl(kvStoreProvider);

      final Orphanage.Factory orphanageFactory = new Orphanage.Factory() {
        @Override
        public Orphanage get() {
          return orphanage;
        }
      };

      final NamespaceService.Factory namespaceServiceFactory = new NamespaceService.Factory() {
        @Override
        public NamespaceService get(String userName) {
          return namespaceService;
        }

        @Override
        public NamespaceService get(NamespaceIdentity identity) {
          return namespaceService;
        }
      };

      final ViewCreatorFactory viewCreatorFactory = new ViewCreatorFactory() {
        @Override
        public ViewCreator get(String userName) {
          return mock(ViewCreator.class);
        }

        @Override
        public void start() throws Exception {
        }

        @Override
        public void close() throws Exception {
        }
      };
      when(sabotContext.getNamespaceServiceFactory())
        .thenReturn(namespaceServiceFactory);
      when(sabotContext.getNamespaceService(anyString()))
        .thenReturn(namespaceService);
      when(sabotContext.getOrphanageFactory())
        .thenReturn(orphanageFactory);
      when(sabotContext.getViewCreatorFactoryProvider())
        .thenReturn(() -> viewCreatorFactory);

      datasetListingService = new DatasetListingServiceImpl(DirectProvider.wrap(namespaceServiceFactory));
      when(sabotContext.getDatasetListing())
        .thenReturn(datasetListingService);

      when(sabotContext.getClasspathScan())
        .thenReturn(CLASSPATH_SCAN_RESULT);

      final LogicalPlanPersistence lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
      when(sabotContext.getLpPersistence())
        .thenReturn(lpp);
      final OptionValidatorListing optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
      final SystemOptionManager som = new SystemOptionManager(optionValidatorListing, lpp, () -> storeProvider, true);
      OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
        .withOptionManager(new DefaultOptionManager(optionValidatorListing))
        .withOptionManager(som)
        .build();

      som.start();
      when(sabotContext.getOptionManager())
        .thenReturn(optionManager);

      when(sabotContext.getKVStoreProvider())
        .thenReturn(storeProvider);
      when(sabotContext.getConfig())
        .thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);
      when(sabotContext.getDremioConfig())
        .thenReturn(dremioConfig);

      allocator = RootAllocatorFactory.newRoot(sabotConfig);
      when(sabotContext.getAllocator())
        .thenReturn(allocator);

      clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
      when(sabotContext.getClusterCoordinator())
        .thenReturn(clusterCoordinator);
      when(sabotContext.getExecutors())
        .thenReturn(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.EXECUTOR)
          .getAvailableEndpoints());
      when(sabotContext.getCoordinators())
        .thenReturn(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR)
          .getAvailableEndpoints());

      when(sabotContext.getRoles())
        .thenReturn(Sets.newHashSet(ClusterCoordinator.Role.MASTER, ClusterCoordinator.Role.COORDINATOR));
      when(sabotContext.isCoordinator())
        .thenReturn(true);

      pool = new CloseableThreadPool("catalog-test");
      fabricService = new FabricServiceImpl(HOSTNAME, 45678, true, THREAD_COUNT, allocator, RESERVATION, MAX_ALLOCATION,
        TIMEOUT, pool);

      final MetadataRefreshInfoBroadcaster broadcaster = mock(MetadataRefreshInfoBroadcaster.class);
      doNothing().when(broadcaster).communicateChange(any());

      catalogService = new CatalogServiceImpl(
        () -> sabotContext,
        () -> new LocalSchedulerService(1),
        () -> new SystemTablePluginConfigProvider(),
        null,
        () -> fabricService,
        () -> ConnectionReader.of(sabotContext.getClasspathScan(), sabotConfig),
        () -> allocator,
        () -> storeProvider,
        () -> datasetListingService,
        () -> optionManager,
        () -> broadcaster,
        dremioConfig,
        EnumSet.allOf(ClusterCoordinator.Role.class),
        () -> new ModifiableLocalSchedulerService(2, "modifiable-scheduler-",
          ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES, () -> optionManager)
      );
      catalogService.start();

      mockUpPlugin = new TestCatalogServiceImpl.MockUpPlugin();
      mockUpKey = new NamespaceKey(TestCatalogServiceImpl.MOCK_UP);

      final SourceConfig mockUpConfig = new SourceConfig()
        .setName(TestCatalogServiceImpl.MOCK_UP)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
        .setCtime(100L)
        .setConnectionConf(new TestCatalogServiceImpl.MockUpConfig(mockUpPlugin));

      catalogService.getSystemUserCatalog().createSource(mockUpConfig);
      datasetCatalogService = new DatasetCatalogServiceImpl(() -> catalogService, () -> namespaceServiceFactory);
    }

    @After
    public void shutdown() throws Exception {
      AutoCloseables.close(catalogService /* closes mockUpPlugin as well */, fabricService, pool, clusterCoordinator,
        allocator, storeProvider);
    }

    @Test
    public void testAddingNewDataset() throws Exception {
      final CapturingStreamObserver<Empty> observer = addDummyDataset();
      validateSuccessfulCall(observer);

      assertTrue(namespaceService.exists(MOCKUP_NEWDATASET));
      DatasetConfig dataset = namespaceService.getDataset(MOCKUP_NEWDATASET);
      assertNotNull(dataset.getName());

      Catalog catalog = catalogService.getCatalog(MetadataRequestOptions.of(
        SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME)).build()));
      assertNotNull(catalog.getTable(MOCKUP_NEWDATASET));
    }

    @Test
    public void testUpdatingDatasetSchemaOnly() throws Exception {
      addDummyDataset();
      final UpdatableDatasetConfigFields oldFields = getDataset(Arrays.asList(TestCatalogServiceImpl.MOCK_UP, NEW_DATASET));
      final UpdatableDatasetConfigFields updateFields = UpdatableDatasetConfigFields.newBuilder()
        .setBatchSchema(UnsafeByteOperations.unsafeWrap(createDummyBatchSchema("renamed_field").toByteString().toByteArray()))
        .setTag(oldFields.getTag())
        .build();

      final AddOrUpdateDatasetRequest request = AddOrUpdateDatasetRequest.newBuilder()
        .setOperationType(OperationType.UPDATE)
        .addDatasetPath(TestCatalogServiceImpl.MOCK_UP)
        .addDatasetPath(NEW_DATASET)
        .setDatasetConfig(updateFields)
        .build();

      final CapturingStreamObserver<Empty> observer = new CapturingStreamObserver<>();
      datasetCatalogService.addOrUpdateDataset(request, observer);
      validateSuccessfulCall(observer);

      final DatasetConfig config = namespaceService.getDataset(MOCKUP_NEWDATASET);
      final BatchSchema actualSchema = BatchSchema.deserialize(config.getRecordSchema().toByteArray());
      assertEquals("renamed_field", actualSchema.getColumn(0).getName());
    }

    @Test
    public void testUpdatingReadDefinitionOnly() throws Exception {
      addDummyDataset();
      final UpdatableDatasetConfigFields oldFields = getDataset(Arrays.asList(TestCatalogServiceImpl.MOCK_UP, NEW_DATASET));
      final UpdatableDatasetConfigFields updateFields = UpdatableDatasetConfigFields.newBuilder()
        .setDatasetType(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET)
        .setReadDefinition(createDummyReadDefinition(DatasetCommonProtobuf.ScanStatsType.NO_EXACT_ROW_COUNT))
        .setTag(oldFields.getTag())
        .build();

      final AddOrUpdateDatasetRequest request = AddOrUpdateDatasetRequest.newBuilder()
        .setOperationType(OperationType.UPDATE)
        .addDatasetPath(TestCatalogServiceImpl.MOCK_UP)
        .addDatasetPath(NEW_DATASET)
        .setDatasetConfig(updateFields)
        .build();

      final CapturingStreamObserver<Empty> observer = new CapturingStreamObserver<>();
      datasetCatalogService.addOrUpdateDataset(request, observer);
      validateSuccessfulCall(observer);

      final DatasetConfig config = namespaceService.getDataset(MOCKUP_NEWDATASET);
      assertEquals(ScanStatsType.NO_EXACT_ROW_COUNT, config.getReadDefinition().getScanStats().getType());
    }

    @Test
    public void testUpdatingReadDefinitionAndSchema() throws Exception {
      addDummyDataset();
      final UpdatableDatasetConfigFields oldFields = getDataset(Arrays.asList(TestCatalogServiceImpl.MOCK_UP, NEW_DATASET));
      final UpdatableDatasetConfigFields updateFields = UpdatableDatasetConfigFields.newBuilder()
        .setBatchSchema(UnsafeByteOperations.unsafeWrap(createDummyBatchSchema("renamed_field").toByteString().toByteArray()))
        .setDatasetType(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET)
        .setReadDefinition(createDummyReadDefinition(DatasetCommonProtobuf.ScanStatsType.NO_EXACT_ROW_COUNT))
        .setTag(oldFields.getTag())
        .build();

      final AddOrUpdateDatasetRequest request = AddOrUpdateDatasetRequest.newBuilder()
        .setOperationType(OperationType.UPDATE)
        .addDatasetPath(TestCatalogServiceImpl.MOCK_UP)
        .addDatasetPath(NEW_DATASET)
        .setDatasetConfig(updateFields)
        .build();

      final CapturingStreamObserver<Empty> observer = new CapturingStreamObserver<>();
      datasetCatalogService.addOrUpdateDataset(request, observer);
      validateSuccessfulCall(observer);

      final DatasetConfig config = namespaceService.getDataset(MOCKUP_NEWDATASET);
      final BatchSchema actualSchema = BatchSchema.deserialize(config.getRecordSchema().toByteArray());
      assertEquals("renamed_field", actualSchema.getColumn(0).getName());
      assertEquals(ScanStatsType.NO_EXACT_ROW_COUNT, config.getReadDefinition().getScanStats().getType());
    }

    private static BatchSchema createDummyBatchSchema(String fieldName) {
      return BatchSchema.newBuilder()
        .addField(new Field(fieldName, FieldType.nullable(ArrowType.Utf8.INSTANCE), null))
        .build();
    }

    private static DatasetCommonProtobuf.ReadDefinition createDummyReadDefinition(DatasetCommonProtobuf.ScanStatsType scanStatsType) {
      return DatasetCommonProtobuf.ReadDefinition.newBuilder()
        .setExtendedProperty(ByteString.EMPTY)
        .setLastRefreshDate(Long.MAX_VALUE)
        .setReadSignature(ByteString.EMPTY)
        .setManifestScanStats(DatasetCommonProtobuf.ScanStats.newBuilder().setType(scanStatsType).setScanFactor(0.5).setCpuCost(0.5f).setCpuCost(0.5f).setRecordCount(1L))
        .setScanStats(DatasetCommonProtobuf.ScanStats.newBuilder().setType(scanStatsType).setScanFactor(0.5).setCpuCost(0.5f).setCpuCost(0.5f).setRecordCount(1L))
        .setSplitVersion(1L)
        .build();
    }

    private static void validateSuccessfulCall(CapturingStreamObserver<? extends AbstractMessage> observer) {
      assertNotNull(observer.value);
      assertNull(observer.exception);
    }

    private CapturingStreamObserver<Empty> addDummyDataset() {
      final UpdatableDatasetConfigFields fields = UpdatableDatasetConfigFields.newBuilder()
        .setFileFormat(FileProtobuf.FileConfig.newBuilder().build())
        .setBatchSchema(UnsafeByteOperations.unsafeWrap(createDummyBatchSchema("dummy_field").toByteString().toByteArray()))
        .setDatasetType(DatasetCommonProtobuf.DatasetType.PHYSICAL_DATASET)
        .setReadDefinition(createDummyReadDefinition(DatasetCommonProtobuf.ScanStatsType.EXACT_ROW_COUNT))
        .build();
      final AddOrUpdateDatasetRequest request = AddOrUpdateDatasetRequest.newBuilder()
        .setOperationType(OperationType.CREATE)
        .addDatasetPath(TestCatalogServiceImpl.MOCK_UP)
        .addDatasetPath(NEW_DATASET)
        .setDatasetConfig(fields)
        .build();

      final CapturingStreamObserver<Empty> observer = new CapturingStreamObserver<>();
      datasetCatalogService.addOrUpdateDataset(request, observer);
      return observer;
    }

    private UpdatableDatasetConfigFields getDataset(List<String> datasetPath) {
      final GetDatasetRequest request = GetDatasetRequest.newBuilder()
        .addAllDatasetPath(datasetPath)
        .build();

      final CapturingStreamObserver<UpdatableDatasetConfigFields> observer = new CapturingStreamObserver<>();
      datasetCatalogService.getDataset(request, observer);
      validateSuccessfulCall(observer);
      return observer.value;
    }
  }

  public static class StatusCodeTranslationTests {
    @Test
    public void testConcurrentModificationException() throws NamespaceException {
      checkGrpcStatusFromError(Status.ABORTED.getCode(), new ConcurrentModificationException());
    }

    @Test
    public void testIllegalArgumentException() throws NamespaceException {
      checkGrpcStatusFromError(Status.INVALID_ARGUMENT.getCode(), new IllegalArgumentException());
    }

    @Test
    public void testUserExceptionInvalidMetadata() throws NamespaceException {
      checkGrpcStatusFromError(Status.ABORTED.getCode(), UserException.invalidMetadataError().buildSilently());
    }

    @Test
    public void testUserExceptionConcurrentModification() throws NamespaceException {
      checkGrpcStatusFromError(Status.ABORTED.getCode(), UserException.concurrentModificationError().buildSilently());
    }

    @Test
    public void testNamespaceNotFoundException() throws NamespaceException {
      checkGrpcStatusFromError(Status.NOT_FOUND.getCode(), new NamespaceNotFoundException(new NamespaceKey("Fake"), "NamespaceNotFoundException"));
    }

    @Test
    public void testNamespaceInvalidStateException() throws NamespaceException {
      checkGrpcStatusFromError(Status.INTERNAL.getCode(), new NamespaceInvalidStateException("NamespaceInvalidStateException"));
    }

    @Test
    public void testUnknownException() throws NamespaceException {
      checkGrpcStatusFromError(Status.UNKNOWN.getCode(), new RuntimeException());
    }

    private static void checkGrpcStatusFromError(Status.Code expectedCode, Throwable exception) throws NamespaceException {
      final CatalogService mockCatalog = mock(CatalogService.class);
      final Catalog mockDatasetCatalog = mock(Catalog.class);
      when(mockCatalog.getCatalog(any())).thenReturn(mockDatasetCatalog);
      final NamespaceService.Factory mockNamespaceFactory = mock(NamespaceService.Factory.class);
      final NamespaceService mockNamespace = mock(NamespaceService.class);
      when(mockNamespaceFactory.get(anyString())).thenReturn(mockNamespace);
      when(mockNamespace.exists(any(), eq(NameSpaceContainer.Type.DATASET))).thenReturn(Boolean.TRUE);
      when(mockNamespace.getDataset(any())).thenThrow(exception);

      final DatasetCatalogServiceImpl service = new DatasetCatalogServiceImpl(() -> mockCatalog, () -> mockNamespaceFactory);
      final CapturingStreamObserver<Empty> streamObserver = new CapturingStreamObserver<>();
      final AddOrUpdateDatasetRequest request = AddOrUpdateDatasetRequest.newBuilder()
        .addDatasetPath("fake")
        .setOperationType(OperationType.UPDATE)
        .setDatasetConfig(UpdatableDatasetConfigFields.newBuilder().build())
        .build();

      service.addOrUpdateDataset(request, streamObserver);

      assertTrue(streamObserver.completed);
      assertNull(streamObserver.value);
      assertNotNull(streamObserver.exception);
      assertEquals(streamObserver.exception.getCause(), exception);
      assertTrue(streamObserver.exception instanceof StatusException);
      assertEquals(((StatusException) streamObserver.exception).getStatus().getCode(), expectedCode);
    }
  }
}
