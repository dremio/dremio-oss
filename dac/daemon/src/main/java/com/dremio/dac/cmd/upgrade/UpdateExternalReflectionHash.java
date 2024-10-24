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
package com.dremio.dac.cmd.upgrade;

import com.dremio.common.Version;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.reflection.DatasetHashUtils;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.inject.Provider;

/** Update all external reflections' query and target dataset hashes. */
public class UpdateExternalReflectionHash extends UpgradeTask implements LegacyUpgradeTask {

  // DO NOT MODIFY
  static final String taskUUID = "79312f25-49d6-40e7-8096-7e132e1b64c4";

  private NamespaceService namespace;
  private Provider<CatalogService> catalogServiceProvider;
  private ExternalReflectionStore store;

  public UpdateExternalReflectionHash() {
    super("Update External Reflections", ImmutableList.of(MinimizeJobResultsMetadata.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_210;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    namespace =
        new NamespaceServiceImpl(
            context.getKvStoreProvider(),
            new CatalogStatusEventsImpl(),
            CatalogEventMessagePublisherProvider.NO_OP);
    store = new ExternalReflectionStore(DirectProvider.wrap(context.getLegacyKVStoreProvider()));

    final Iterable<ExternalReflection> reflections = store.getExternalReflections();
    StreamSupport.stream(reflections.spliterator(), false).forEach(this::update);
  }

  private void update(ExternalReflection reflection) {
    try {
      final String queryDatasetId = reflection.getQueryDatasetId();
      reflection.setQueryDatasetHash(computeDatasetHash(new EntityId(queryDatasetId)));

      final String targetDatasetId = reflection.getTargetDatasetId();
      reflection.setTargetDatasetHash(computeDatasetHash(new EntityId(targetDatasetId)));

      AdminLogger.log("  Updated external reflection {}", reflection.getId());
      store.addExternalReflection(reflection);
    } catch (Exception e) {
      AdminLogger.log("  Failed to update external reflection {}", reflection.getId());
    }
  }

  /**
   * @return dataset hash, if dataset exists in the namespace (along with all its ancestors if its a
   *     VDS). null otherwise
   */
  private Integer computeDatasetHash(EntityId datasetId) {
    Optional<DatasetConfig> dataset = namespace.getDatasetById(datasetId);
    if (dataset.isEmpty()) {
      return null;
    }

    try {
      EntityExplorer catalog =
          CatalogUtil.getSystemCatalogForReflections(catalogServiceProvider.get());
      return DatasetHashUtils.computeDatasetHash(dataset.get(), catalog, false);
    } catch (NamespaceException e) {
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
