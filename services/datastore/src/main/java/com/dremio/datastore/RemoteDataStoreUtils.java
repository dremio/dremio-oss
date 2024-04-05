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

import com.dremio.datastore.RemoteDataStoreProtobuf.PutRequestIndexKey;
import com.dremio.datastore.RemoteDataStoreProtobuf.SearchRequest;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.datastore.indexed.IndexKey;

/** Utilities related to remote invocation of datastore. */
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
   * @param storeId store id
   * @param condition find by condition
   * @return search request
   */
  public static SearchRequest getRequestFromCondition(
      String storeId, LegacyFindByCondition condition) {
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

  /**
   * Converts an {@link IndexKey} to {@link PutRequestIndexKey} for use with remote datastore
   * PutRequests in indexed stores, for use with DocumentWriters such as {@link
   * com.dremio.datastore.indexed.SimpleDocumentWriter}.
   *
   * @param indexKey The index key to convert.
   * @return The PutRequestIndexKey that can be attached to PutRequest api calls.
   */
  public static PutRequestIndexKey toPutRequestIndexKey(IndexKey indexKey) {
    PutRequestIndexKey.Builder builder =
        PutRequestIndexKey.newBuilder()
            .setShortName(indexKey.getShortName())
            .setIndexFieldName(indexKey.getIndexFieldName())
            .setStored(indexKey.isSorted())
            .setCanContainMultipleValues(indexKey.canContainMultipleValues());

    SearchTypes.SearchFieldSorting.FieldType sortedValueType = indexKey.getSortedValueType();
    if (null != sortedValueType) {
      builder.setSortingValueType(sortedValueType);
    }

    Class<?> valueType = indexKey.getValueType();
    if (valueType == String.class) {
      // bytes are labelled with valuetype of String in SimpleDocumentConverter
      builder.setValueType(RemoteDataStoreProtobuf.PutRequestIndexKeyValueType.STRING);
    } else if (valueType == Long.class) {
      builder.setValueType(RemoteDataStoreProtobuf.PutRequestIndexKeyValueType.LONG);
    } else if (valueType == Integer.class) {
      builder.setValueType(RemoteDataStoreProtobuf.PutRequestIndexKeyValueType.INTEGER);
    } else if (valueType == Double.class) {
      builder.setValueType(RemoteDataStoreProtobuf.PutRequestIndexKeyValueType.DOUBLE);
    } else {
      throw new IllegalStateException(
          String.format("Unknown index key value type: %s", valueType.getName()));
    }

    return builder.build();
  }

  /**
   * Converts a {@link PutRequestIndexKey} to an {@link IndexKey} for use with DocumentWriters.
   *
   * @param requestIndexKey The PutRequestIndexKey which was attached to a PutRequest api call.
   * @return The index key to be used with DocumentWriters such as {@link
   *     com.dremio.datastore.indexed.SimpleDocumentWriter}.
   */
  public static IndexKey toIndexKey(PutRequestIndexKey requestIndexKey) {

    final Class<?> valueType;
    switch (requestIndexKey.getValueType()) {
      case INTEGER:
        valueType = Integer.class;
        break;
      case DOUBLE:
        valueType = Double.class;
        break;
      case LONG:
        valueType = Long.class;
        break;
      case STRING:
        valueType = String.class;
        break;
      default:
        throw new IllegalStateException(
            String.format("Unknown index key type: %s", requestIndexKey.getValueType().name()));
    }

    IndexKey.Builder builder =
        IndexKey.newBuilder(
                requestIndexKey.getShortName(), requestIndexKey.getIndexFieldName(), valueType)
            .setStored(requestIndexKey.getStored())
            .setCanContainMultipleValues(requestIndexKey.getCanContainMultipleValues());

    if (requestIndexKey.hasSortingValueType()) {
      switch (requestIndexKey.getSortingValueType()) {
        case STRING:
          builder.setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.STRING);
          break;
        case LONG:
          builder.setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.LONG);
          break;
        case DOUBLE:
          builder.setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.DOUBLE);
          break;
        case INTEGER:
          builder.setSortedValueType(SearchTypes.SearchFieldSorting.FieldType.INTEGER);
          break;
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown index key sorting value type: %s",
                  requestIndexKey.getSortingValueType().name()));
      }
    }

    return builder.build();
  }

  private RemoteDataStoreUtils() {}
}
