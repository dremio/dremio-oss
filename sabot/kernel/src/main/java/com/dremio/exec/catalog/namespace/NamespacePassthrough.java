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
package com.dremio.exec.catalog.namespace;

import com.dremio.catalog.model.CatalogEntityId;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByCondition;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import java.util.List;

/** An interface to abstract NamespaceService away from the architectural layers above Catalog. */
public interface NamespacePassthrough {

  boolean existsById(CatalogEntityId id);

  List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys);

  void addOrUpdateDataset(
      NamespaceKey datasetPath, DatasetConfig dataset, NamespaceAttribute... attributes)
      throws NamespaceException;

  DatasetConfig renameDataset(NamespaceKey oldDatasetPath, NamespaceKey newDatasetPath)
      throws NamespaceException;

  String getEntityIdByPath(NamespaceKey entityPath) throws NamespaceNotFoundException;

  NameSpaceContainer getEntityByPath(NamespaceKey entityPath) throws NamespaceException;

  DatasetConfig getDataset(NamespaceKey datasetPath) throws NamespaceException;

  Iterable<NamespaceKey> getAllDatasets(final NamespaceKey parent) throws NamespaceException;

  void deleteDataset(NamespaceKey datasetPath, String version, NamespaceAttribute... attributes)
      throws NamespaceException;

  List<Integer> getCounts(SearchTypes.SearchQuery... queries) throws NamespaceException;

  Iterable<Document<NamespaceKey, NameSpaceContainer>> find(FindByCondition condition);

  List<NameSpaceContainer> getEntitiesByIds(List<EntityId> ids);
}
