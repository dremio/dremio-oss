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
package com.dremio.service.orphanagecleaner;


import static com.dremio.common.UserConstants.SYSTEM_USERNAME;
import static com.dremio.common.utils.PathUtils.constructFullPath;
import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.QueryType;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.orphanage.OrphanageEntryHandler;
import com.dremio.service.orphanage.proto.OrphanEntry;
import com.google.common.base.Preconditions;

/**
 *Handler  for the internal iceberg metadata orphans
 */
public class IcebergMetadataHandler implements OrphanageEntryHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergMetadataHandler.class);

  private final NamespaceService namespaceService;
  private final SabotContext sabotContext;


  public IcebergMetadataHandler(NamespaceService namespaceService,  SabotContext sabotContext) {
    this.namespaceService = namespaceService;
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "sabot context required");
  }


  private void runDropQuery(String query, String user, String queryType) throws Exception {
    sabotContext.getJobsRunner().get().runQueryAsJob(query, user, queryType);
  }


  private void deleteIcebergMetadataFromNamespace(OrphanEntry.OrphanIcebergMetadata val) throws Exception {

    NamespaceKey namespaceKey = new NamespaceKey(val.getDatasetFullPathList());
    try {
      DatasetConfig dataset;
      try {
        dataset = namespaceService.getDataset(namespaceKey);
      } catch (NamespaceNotFoundException ex) {
        dataset = null;
      } catch (NamespaceException ex) {
        throw new RuntimeException(ex);
      }

      if(dataset == null ) {
        logger.debug("Unable to find dataset in namespace {}", namespaceKey);
        return ;
      }

      namespaceService.deleteDataset(namespaceKey, val.getDatasetTag());

    } catch (Exception e) {
      logger.debug("Deleting the entry {} {} from the namespace caused an error", namespaceKey, val.getDatasetTag(), e);
      throw e;
    }
  }

  @Override
  public boolean process(OrphanEntry.OrphanId key, OrphanEntry.Orphan val) {
    String icebergTableUuid = "";
    try {
      logger.debug("Processing of iceberg metadata with key {} started in iceberg metadata handler", key);
      OrphanEntry.OrphanIcebergMetadata orphanValue = OrphanEntry.OrphanIcebergMetadata.parseFrom(val.getOrphanDetails());
      icebergTableUuid = orphanValue.getIcebergTableUuid();
      List path = new ArrayList();
      path.add(METADATA_STORAGE_PLUGIN_NAME);
      path.add(icebergTableUuid);
      deleteIcebergMetadataFromNamespace(orphanValue);
      final String pathString = constructFullPath(path);
      final String query = String.format("DROP TABLE IF EXISTS %s", pathString);
      //Submitting drop query to delete the iceberg and nessie entry of the metadata
      runDropQuery(query, SYSTEM_USERNAME, QueryType.INTERNAL_ICEBERG_METADATA_DROP.toString());
    } catch (Exception e) {
      logger.warn("Processing the iceberg metadata cleanup with table id {} from the orphanage caused an error", icebergTableUuid, e);
      return false;
    }
    return true;
  }


}
