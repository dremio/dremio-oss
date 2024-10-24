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
package com.dremio.dac.service.admin;

import com.dremio.common.DeferredException;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceStore;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.MultiSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.inject.Provider;

/** service responsible for getting the split info from split stores and reflection stores */
public class KVStoreReportService implements Service {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(KVStoreReportService.class);
  public static final int BUFFER_SIZE = 4096;
  public static final String MULTI_SPLITS = "metadata-multi-splits";
  public static final String DATASET_SPLITS = "metadata-dataset-splits";
  public static final String NAMESPACE = "dac-namespace";
  public static final String MATERIALIZATION = "materialization-store";
  public static final String REFLECTION_GOALS = "reflection-goals";
  public static final String REFLECTION_ENTRIES = "reflection-entries";
  public static final String NO_ANALYSIS =
      "none"; // returns a report of kvstore stats and sources config, not performing any analysis
  // on any store.
  public static final Set<String> SUPPORT_STORES =
      ImmutableSet.of(
          MULTI_SPLITS,
          DATASET_SPLITS,
          NAMESPACE,
          MATERIALIZATION,
          REFLECTION_GOALS,
          REFLECTION_ENTRIES);
  public static final ObjectMapper MAPPER =
      new ObjectMapper(new JsonFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false))
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final Provider<ExecutorService> executorServiceProvider;
  private ListeningExecutorService executorService;

  private final Provider<LegacyKVStoreProvider> legacyStoreProviderProvider;
  private final Provider<KVStoreProvider> storeProviderProvider;
  private final Provider<NamespaceService> namespaceServiceProvider;

  public KVStoreReportService(
      Provider<LegacyKVStoreProvider> legacyStoreProviderProvider,
      Provider<KVStoreProvider> storeProviderProvider,
      Provider<NamespaceService> namespaceServiceProvider,
      Provider<ExecutorService> executorServiceProvider) {
    this.legacyStoreProviderProvider = legacyStoreProviderProvider;
    this.storeProviderProvider = storeProviderProvider;
    this.namespaceServiceProvider = namespaceServiceProvider;
    this.executorServiceProvider = executorServiceProvider;
  }

  @Override
  public void start() {
    this.executorService = MoreExecutors.listeningDecorator(executorServiceProvider.get());
  }

  @Override
  public void close() {}

  public InputStream getSplitReport(List<String> storeNames)
      throws IOException, KVStoreNotSupportedException {
    final PipedOutputStream pipeOs = new PipedOutputStream();
    final PipedInputStream pipeIs = new PipedInputStream(pipeOs, BUFFER_SIZE);
    Set<String> deduplicatedStoreNames = validateStoreNames(storeNames);

    ListenableFuture<Void> future =
        executorService.submit(
            () -> {
              try (ZipOutputStream zip = new ZipOutputStream(pipeOs)) {
                DeferredException exceptionsCollector = new DeferredException();

                for (String name : deduplicatedStoreNames) {
                  switch (name) {
                    case MULTI_SPLITS:
                      getMultiSplitsReport(zip, exceptionsCollector);
                      break;
                    case DATASET_SPLITS:
                      getDatasetSplitsReport(zip, exceptionsCollector);
                      break;
                    case NAMESPACE:
                      getNamespaceReport(zip, exceptionsCollector);
                      break;
                    case MATERIALIZATION:
                      getMaterializationsReport(zip, exceptionsCollector);
                      break;
                    case REFLECTION_GOALS:
                      getReflectionGoalsReport(zip, exceptionsCollector);
                      break;
                    case REFLECTION_ENTRIES:
                      getReflectionEntriesReport(zip, exceptionsCollector);
                      break;
                    default:
                      break;
                  }
                }
                getSourceConfig(zip, exceptionsCollector);
                getKVStoresStats(zip, exceptionsCollector);
                getErrorReport(zip, exceptionsCollector);
                exceptionsCollector.close();
              } catch (Exception e) {
                logger.error("Unexpected error: ", e);
              }
              return null;
            });

    return pipeIs;
  }

  private Set<String> validateStoreNames(List<String> storeNames)
      throws KVStoreNotSupportedException {
    if (storeNames == null || storeNames.isEmpty()) { // default: print all stores
      return SUPPORT_STORES;
    }
    for (String name : storeNames) {
      if (!SUPPORT_STORES.contains(name) && !NO_ANALYSIS.equals(name)) {
        throw new KVStoreNotSupportedException(name);
      }
    }
    return ImmutableSet.copyOf(storeNames);
  }

  private void getMultiSplitsReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(MULTI_SPLITS + ".csv"));

      zip.write(
          "dataset_id,split_version,split_id,value_size,split_data_size,split_count,multi_split_key\n"
              .getBytes()); // column names
      storeProviderProvider
          .get()
          .getStore(NamespaceServiceImpl.MultiSplitStoreCreator.class)
          .find()
          .forEach(
              e -> {
                PartitionChunkId key = e.getKey();
                MultiSplit value = e.getValue();
                try {
                  zip.write(
                      String.format(
                              "%s,%s,%s,%d,%d,%d,%s\n",
                              key.getDatasetId(),
                              key.getSplitVersion(),
                              key.getSplitIdentifier(),
                              value.getSerializedSize(), // important
                              (value.getSplitData() != null) ? value.getSplitData().size() : 0,
                              value.getSplitCount(),
                              value.getMultiSplitKey())
                          .getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getDatasetSplitsReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(DATASET_SPLITS + ".csv"));

      zip.write(
          "dataset_id,split_version,split_id,row_count,partition_extended_property_length,dataset_split_size,value_size,split_key,split_count\n"
              .getBytes());
      storeProviderProvider
          .get()
          .getStore(NamespaceServiceImpl.PartitionChunkCreator.class)
          .find()
          .forEach(
              e -> {
                PartitionChunkId key = e.getKey();
                PartitionChunk value = e.getValue();
                try {
                  zip.write(
                      String.format(
                              "%s,%s,%s,%d,%d,%d,%d,%s,%d\n",
                              key.getDatasetId(),
                              key.getSplitVersion(),
                              key.getSplitIdentifier(),
                              value.getRowCount(),
                              (value.getPartitionExtendedProperty() != null)
                                  ? value.getPartitionExtendedProperty().size()
                                  : 0,
                              (value.getDatasetSplit() != null)
                                  ? value.getDatasetSplit().getSerializedSize()
                                  : 0,
                              value.getSerializedSize(),
                              value.getSplitKey(),
                              value.getSplitCount())
                          .getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getNamespaceReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(NAMESPACE + ".csv"));

      zip.write(
          "physical_dataset_path,dataset_id,split_version,signature_length,extended_property_length,record_schema_length,num_splits\n"
              .getBytes());
      new NamespaceStore(storeProviderProvider)
          .find()
          .forEach(
              e -> {
                NameSpaceContainer value = e.getValue();
                if (value.getType() == Type.DATASET) {
                  DatasetConfig datasetConfig = value.getDataset();
                  if (datasetConfig.getPhysicalDataset() != null) {
                    try {
                      zip.write(
                          String.format(
                                  "%s,%s,%s,%d,%d,%d,%d\n",
                                  String.join("/", value.getFullPathList()),
                                  (datasetConfig.getId() != null)
                                      ? datasetConfig.getId().getId()
                                      : "null",
                                  (datasetConfig.getReadDefinition() != null)
                                      ? datasetConfig.getReadDefinition().getSplitVersion()
                                      : "null",
                                  (datasetConfig.getReadDefinition() != null
                                          && datasetConfig.getReadDefinition().getReadSignature()
                                              != null)
                                      ? datasetConfig.getReadDefinition().getReadSignature().size()
                                      : 0,
                                  (datasetConfig.getReadDefinition() != null
                                          && datasetConfig.getReadDefinition().getExtendedProperty()
                                              != null)
                                      ? datasetConfig
                                          .getReadDefinition()
                                          .getExtendedProperty()
                                          .size()
                                      : 0,
                                  (datasetConfig.getRecordSchema() != null)
                                      ? datasetConfig.getRecordSchema().size()
                                      : 0,
                                  datasetConfig.getTotalNumSplits())
                              .getBytes());
                    } catch (Exception ex) {
                      exceptionsCollector.addException(ex);
                    }
                  }
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getMaterializationsReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(MATERIALIZATION + ".csv"));

      zip.write(
          "materialization_id,reflection_id,state,base_path,reflection_goal_version,created_at,modified_at,expiration,last_refresh_from_pds,num_partitions\n"
              .getBytes());
      new MaterializationStore(legacyStoreProviderProvider)
          .getAllMaterializations()
          .forEach(
              e -> {
                try {
                  zip.write(
                      String.format(
                              "%s,%s,%s,%s,%s,%d,%d,%d,%d,%d\n",
                              e.getId().getId(),
                              (e.getReflectionId() != null) ? e.getReflectionId().getId() : "null",
                              e.getState(),
                              e.getBasePath(),
                              e.getReflectionGoalVersion(),
                              e.getCreatedAt(),
                              e.getModifiedAt(),
                              e.getExpiration(),
                              e.getLastRefreshFromPds(),
                              (e.getPartitionList() != null) ? e.getPartitionList().size() : 0)
                          .getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getReflectionGoalsReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(REFLECTION_GOALS + ".csv"));

      zip.write(
          "reflection_id,dataset_id,name,state,type,created_at,modified_at,version\n".getBytes());
      new ReflectionGoalsStore(legacyStoreProviderProvider)
          .getAll()
          .forEach(
              e -> {
                try {
                  zip.write(
                      String.format(
                              "%s,%s,%s,%s,%s,%d,%d,%d\n",
                              (e.getId() != null) ? e.getId().getId() : "null",
                              e.getDatasetId(),
                              e.getName(),
                              e.getState(),
                              e.getType(),
                              e.getCreatedAt(),
                              e.getModifiedAt(),
                              e.getVersion())
                          .getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getReflectionEntriesReport(
      ZipOutputStream zip, DeferredException exceptionsCollector) throws IOException {
    try {
      zip.putNextEntry(new ZipEntry(REFLECTION_ENTRIES + ".csv"));

      zip.write(
          "reflection_id,dataset_id,name,state,type,created_at,modified_at,goal_version\n"
              .getBytes());
      new ReflectionEntriesStore(legacyStoreProviderProvider)
          .find()
          .forEach(
              e -> {
                try {
                  zip.write(
                      String.format(
                              "%s,%s,%s,%s,%s,%d,%d,%s\n",
                              (e.getId() != null) ? e.getId().getId() : "null",
                              e.getDatasetId(),
                              e.getName(),
                              e.getState(),
                              e.getType(),
                              e.getCreatedAt(),
                              e.getModifiedAt(),
                              e.getGoalVersion())
                          .getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getSourceConfig(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry("sources.json"));

      MAPPER.writeValue(
          zip,
          namespaceServiceProvider.get().getSources().stream()
              .flatMap(
                  sourceConfig -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", sourceConfig.getName());
                    map.put("type", sourceConfig.getType());
                    map.put("metadataPolicy", sourceConfig.getMetadataPolicy());
                    map.put(
                        "accelerationRefreshPeriod", sourceConfig.getAccelerationRefreshPeriod());
                    map.put(
                        "accelerationRefreshSchedule",
                        sourceConfig.getAccelerationRefreshSchedule());
                    map.put(
                        "accelerationActivePolicyType",
                        sourceConfig.getAccelerationActivePolicyType());
                    map.put("accelerationGracePeriod", sourceConfig.getAccelerationGracePeriod());
                    map.put("accelerationNeverExpire", sourceConfig.getAccelerationNeverExpire());
                    map.put("accelerationNeverRefresh", sourceConfig.getAccelerationNeverRefresh());
                    map.put(
                        "accelerationRefreshOnDataChanges",
                        sourceConfig.getAccelerationRefreshOnDataChanges());
                    return Stream.of(map);
                  })
              .collect(Collectors.toList()));

    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getKVStoresStats(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    try {
      zip.putNextEntry(new ZipEntry("kvstores_stats.log"));
      storeProviderProvider
          .get()
          .unwrap(LocalKVStoreProvider.class)
          .forEach(
              storeWithId -> {
                try {
                  zip.write(storeWithId.getStore().getAdmin().getStats().getBytes());
                } catch (Exception ex) {
                  exceptionsCollector.addException(ex);
                }
              });
    } catch (Exception ex) {
      exceptionsCollector.addException(ex);
    }
    zip.closeEntry();
  }

  private void getErrorReport(ZipOutputStream zip, DeferredException exceptionsCollector)
      throws IOException {
    if (exceptionsCollector.hasException()) {
      zip.putNextEntry(new ZipEntry("error_report.log"));
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      exceptionsCollector.getException().printStackTrace(pw);
      zip.write(sw.toString().getBytes());
      zip.closeEntry();
    }
  }

  public static class KVStoreNotSupportedException extends Exception {
    public KVStoreNotSupportedException(String storeName) {
      super(String.format("kv store %s is not supported by report tool.", storeName));
    }
  }
}
