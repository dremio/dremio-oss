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
package com.dremio.datastore;

import com.dremio.datastore.RemoteDataStoreProtobuf.SearchRequest;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;

/**
 * Utilities related to remote invocation of datastore.
 */
public final class RemoteDataStoreUtils {

  /**
   * Converts a {@link SearchRequest} to a {@link LegacyFindByCondition}.
   *
   * @param searchRequest search request
   * @return find by condition
   */
  public static LegacyFindByCondition getConditionFromRequest(SearchRequest searchRequest) {
    final LegacyFindByCondition findByCondition = new LegacyFindByCondition();
    if (searchRequest.hasLimit()) {
      findByCondition.setLimit(searchRequest.getLimit());
    }
    if (searchRequest.hasOffset()) {
      findByCondition.setOffset(searchRequest.getOffset());
    }
    if (searchRequest.hasPageSize()) {
      findByCondition.setPageSize(searchRequest.getPageSize());
    }
    if (searchRequest.getSortCount() != 0) {
      findByCondition.addSortings(searchRequest.getSortList());
    }
    if (searchRequest.hasQuery()) {
      findByCondition.setCondition(searchRequest.getQuery());
    }
    return findByCondition;
  }

  /**
   * Converts a {@link LegacyFindByCondition} to a {@link SearchRequest}.
   *
   * @param storeId   store id
   * @param condition find by condition
   * @return search request
   */
  public static SearchRequest getRequestFromCondition(String storeId, LegacyFindByCondition condition) {
    final SearchRequest.Builder builder = SearchRequest.newBuilder();
    builder.setStoreId(storeId);
    if (condition.getCondition() != null) {
      builder.setQuery(condition.getCondition());
    }
    builder.setLimit(condition.getLimit());
    builder.setOffset(condition.getOffset());
    builder.setPageSize(condition.getPageSize());
    if (!condition.getSort().isEmpty()) {
      builder.addAllSort(condition.getSort());
    }

    return builder.build();
  }

  private RemoteDataStoreUtils() {
  }
}
