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
package com.dremio.service.namespace;

import java.util.List;
import java.util.Map;

import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.service.namespace.proto.NameSpaceContainer;

/**
 * Namespace operations for generic entities. If you are operating on a specific entity, use that entity's
 * NamespaceService.For example, if getting a function, use
 * {@link com.dremio.service.namespace.function.FunctionNamespaceService}.
 */
public interface EntityNamespaceService {
  //// READ
  boolean exists(NamespaceKey key, NameSpaceContainer.Type type);
  boolean exists(NamespaceKey key);
  boolean hasChildren(NamespaceKey key);

  /**
   * Returns entity id by path
   *
   * @param entityPath
   * @return a data set entity id or null, if there is no entityPath by provided path
   */
  String getEntityIdByPath(NamespaceKey entityPath) throws NamespaceNotFoundException;
  NameSpaceContainer getEntityById(String id) throws NamespaceNotFoundException;
  List<NameSpaceContainer> getEntitiesByIds(List<String> ids) throws NamespaceNotFoundException;

  /**
   * Returns an entity given its path.
   *
   * @param entityPath namespace key
   * @return entity associated with this path or null, if there is no entity.
   */
  NameSpaceContainer getEntityByPath(NamespaceKey entityPath) throws NamespaceException;

  /**
   * Get multiple entities of given type
   * @param lookupKeys namespace keys
   * @return list of namespace containers with null if no value found for a key.
   *         Order of returned list matches with order of lookupKeys.
   * @throws NamespaceNotFoundException
   */
  List<NameSpaceContainer> getEntities(List<NamespaceKey> lookupKeys) throws NamespaceNotFoundException;

  /**
   * Return list of counts matching each query
   * @param queries list of queries to perform search on
   * @return list of counts. Order of returned counts is same as order of queries.
   * @throws NamespaceException
   */
  List<Integer> getCounts(SearchTypes.SearchQuery... queries) throws NamespaceException;
  List<NameSpaceContainer> list(NamespaceKey entityPath) throws NamespaceException;
  Iterable<NameSpaceContainer> getAllDescendants(final NamespaceKey root);

  /**
   * Find entries by condition. If condition is not provided, returns all items.
   * @param condition
   * @return List of Key/Container entries.
   */
  Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> find(LegacyIndexedStore.LegacyFindByCondition condition);

  //// DELETE
  /**
   * Do not use. Leverage an entity-specific deletion.
   */
  @Deprecated
  void deleteEntity(NamespaceKey entityPath) throws NamespaceException;

  //// OTHER
  default void invalidateNamespaceCache(final NamespaceKey key) {}
}
