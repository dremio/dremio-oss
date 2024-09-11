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
package com.dremio.dac.service.nodeshistory;

import com.dremio.dac.util.DatasetsUtil;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.plugins.nodeshistory.NodesHistoryTable;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.FileFormat;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.physicaldataset.proto.PhysicalDatasetConfig;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import java.time.Instant;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for creating/updating the dataset exposed by the nodes history system source The
 * folder ("metrics") backing the dataset does not have to already exists. If it doesn't, the
 * created table will be shallow (incomplete metadata); if it does, it will be full.
 */
public class NodesHistoryPluginInitializer {
  private static final Logger logger = LoggerFactory.getLogger(NodesHistoryPluginInitializer.class);
  private static final String LOCAL_TASK_LEADER_NAME = "NodesHistoryPluginInitializer";
  private static final FileType TABLE_FILE_TYPE = FileType.CSV;

  private final Provider<CatalogService> catalogServiceProvider;
  private final Provider<SchedulerService> schedulerServiceProvider;

  @Inject
  public NodesHistoryPluginInitializer(
      Provider<CatalogService> catalogServiceProvider,
      Provider<SchedulerService> schedulerServiceProvider) {
    this.catalogServiceProvider = catalogServiceProvider;
    this.schedulerServiceProvider = schedulerServiceProvider;
  }

  public void initialize() {
    schedulerServiceProvider
        .get()
        .schedule(
            // The task is scheduled using a clustered singleton to ensure only one coordinator is
            // responsible for creating the dataset in KV store
            Schedule.Builder.singleShotChain()
                .startingAt(Instant.now())
                .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                .build(),
            new Runnable() {
              private static final String TASK_NAME = "Create __nodeHistory.metrics system dataset";

              @Override
              public void run() {
                logger.debug("Running task: {}", TASK_NAME);

                List<String> pathList = NodesHistoryTable.METRICS.getPath();
                FileConfig config =
                    new FileConfig()
                        .setFullPathList(pathList)
                        .setName(NodesHistoryTable.METRICS.getName())
                        .setType(TABLE_FILE_TYPE);
                FileFormat folderFormat = FileFormat.getForFolder(config);

                // The physical dataset is a folder of CSV files
                PhysicalDatasetConfig physicalDatasetConfig = new PhysicalDatasetConfig();
                physicalDatasetConfig.setName(config.getName());
                physicalDatasetConfig.setFormatSettings(folderFormat.asFileConfig());
                physicalDatasetConfig.setType(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
                physicalDatasetConfig.setFullPathList(pathList);

                NamespaceKey key = new NamespaceKey(pathList);
                MetadataRequestOptions metadataOptions =
                    MetadataRequestOptions.newBuilder()
                        .setNeverPromote(true)
                        .setCheckValidity(false)
                        .setSchemaConfig(
                            SchemaConfig.newBuilder(CatalogUser.from(SystemUser.SYSTEM_USERNAME))
                                .build())
                        .build();
                Catalog catalog = catalogServiceProvider.get().getCatalog(metadataOptions);
                if (catalog.getTableNoResolve(key) != null) {
                  logger.debug("__nodeHistory.metrics dataset already exists, skipping creation");
                  return;
                }

                try {
                  catalog.createOrUpdateDataset(
                      new NamespaceKey(key.getRoot()),
                      key,
                      DatasetsUtil.toDatasetConfig(
                          physicalDatasetConfig, SystemUser.SYSTEM_USERNAME));
                } catch (NamespaceException e) {
                  throw new RuntimeException(e);
                }
                logger.info("Task completed successfully: {}", TASK_NAME);
              }

              @Override
              public String toString() {
                return TASK_NAME;
              }
            });
  }
}
