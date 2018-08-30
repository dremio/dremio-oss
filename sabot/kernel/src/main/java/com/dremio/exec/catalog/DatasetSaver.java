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

import java.util.List;
import java.util.UUID;

import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.proto.EntityId;

/**
 * Simple facade around namespace that is responsible for persisting datasets. We do this to ensure
 * that system namespace access doesn't leak into user context except for persistence purposes (we
 * always persist as a system user but we need to retrieve as the query user).
 */
public class DatasetSaver {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetSaver.class);

  private final NamespaceService systemUserNamespaceService;
  private final MetadataUpdateListener updateListener;

  public DatasetSaver(
      NamespaceService systemUserNamespaceService,
      MetadataUpdateListener updateListener) {
    this.systemUserNamespaceService = systemUserNamespaceService;
    this.updateListener = updateListener;
  }

  void shallowSave(SourceTableDefinition accessor) throws NamespaceException{
    NamespaceKey key = accessor.getName();
    DatasetConfig shallow = new DatasetConfig();
    shallow.setId(new EntityId().setId(UUID.randomUUID().toString()));
    shallow.setCreatedAt(System.currentTimeMillis());
    shallow.setName(key.getName());
    shallow.setFullPathList(key.getPathComponents());
    shallow.setType(accessor.getType());
    shallow.setSchemaVersion(DatasetHelper.CURRENT_VERSION);
    systemUserNamespaceService.addOrUpdateDataset(key, shallow);
  }

  void completeSave(SourceTableDefinition accessor, DatasetConfig oldConfig, NamespaceAttribute... attributes){
    try {
      DatasetConfig newConfig = accessor.getDataset();
      if (oldConfig != null) {
        NamespaceUtils.copyFromOldConfig(oldConfig, newConfig);
      }
      completeSave(newConfig, accessor.getSplits(), attributes);
    }catch(Exception ex){
      logger.warn("Failure while retrieving and saving dataset {}.", accessor.getName(), ex);
    }
  }

  DatasetConfig completeSave(DatasetConfig newConfig, List<DatasetSplit> splits, NamespaceAttribute... attributes) throws NamespaceException{
    NamespaceKey key = new NamespaceKey(newConfig.getFullPathList());
    systemUserNamespaceService.addOrUpdateDataset(key, newConfig, splits, attributes);
    updateListener.metadataUpdated(key);
    return newConfig;
  }
}
