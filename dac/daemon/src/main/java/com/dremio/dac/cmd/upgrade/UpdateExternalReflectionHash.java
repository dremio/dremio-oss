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
package com.dremio.dac.cmd.upgrade;

import java.util.stream.StreamSupport;

import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.store.ExternalReflectionStore;

/**
 * Update all external reflections' query and target dataset hashes.
 */
public class UpdateExternalReflectionHash extends UpgradeTask {

  private NamespaceService namespace;
  private ExternalReflectionStore store;

  public UpdateExternalReflectionHash() {
    super("Update External Reflections", VERSION_106, VERSION_210, NORMAL_ORDER + 13);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    namespace = new NamespaceServiceImpl(context.getKVStoreProvider());
    store = new ExternalReflectionStore(DirectProvider.wrap(context.getKVStoreProvider()));

    final Iterable<ExternalReflection> reflections = store.getExternalReflections();
    StreamSupport.stream(reflections.spliterator(), false)
      .forEach(this::update);
  }

  private void update(ExternalReflection reflection) {
    try {
      final String queryDatasetId = reflection.getQueryDatasetId();
      reflection.setQueryDatasetHash(computeDatasetHash(queryDatasetId));

      final String targetDatasetId = reflection.getTargetDatasetId();
      reflection.setTargetDatasetHash(computeDatasetHash(targetDatasetId));

      System.out.printf("  Updated external reflection %s%n", reflection.getId());
      store.addExternalReflection(reflection);
    } catch (Exception e) {
      System.err.printf("  Failed to update external reflection %s%n", reflection.getId());
    }
  }

  /**
   * @return dataset hash, if dataset exists in the namespace (along with all its ancestors if its a VDS). null otherwise
   */
  private Integer computeDatasetHash(String datasetId) {
    final DatasetConfig dataset = namespace.findDatasetByUUID(datasetId);
    if (dataset == null) {
      return null;
    }

    try {
      return ReflectionUtils.computeDatasetHash(dataset, namespace);
    } catch (NamespaceException e) {
      return null;
    }
  }
}
