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
package com.dremio.dac.cmd.upgrade;

import java.util.Map.Entry;

import com.dremio.service.namespace.DatasetSplitId;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Rewrite hive metadata so that partition properties are cached and read signature is smaller in size.
 */
class FixHiveMetadata extends UpgradeTask {
  FixHiveMetadata() {
    super("Rewriting Hive Metadata", VERSION_106, VERSION_107);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    try {
      for (SourceConfig source : namespaceService.getSources()) {
        if (source.getType() == SourceType.HIVE) {
          for (NamespaceKey datasetPath : namespaceService.getAllDatasets(new NamespaceKey(source.getName()))) {
            final DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath);

            // drop the metadata and splits
            if (datasetConfig.getReadDefinition() != null) {
              Iterable<DatasetSplitId> splitIdList = Iterables.transform(
                namespaceService.findSplits(DatasetSplitId.getAllSplitsRange(datasetConfig)),
                new Function<Entry<DatasetSplitId, DatasetSplit>, DatasetSplitId>() {
                  @Override
                  public DatasetSplitId apply(Entry<DatasetSplitId, DatasetSplit> input) {
                    return input.getKey();
                  }
                });
              namespaceService.deleteSplits(splitIdList);
            }

            datasetConfig.setReadDefinition(null);
            namespaceService.addOrUpdateDataset(datasetPath, datasetConfig);
          }
        }
      }
    } catch (NamespaceException e) {
      throw new RuntimeException("FixHiveMetadata failed", e);
    }
  }
}
