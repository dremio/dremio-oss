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
package com.dremio.datastore.indexed;

import com.dremio.datastore.RemoteDataStoreProtobuf;
import com.dremio.datastore.RemoteDataStoreUtils;
import com.dremio.datastore.api.DocumentWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Conforms to document writer interface to intercept values. Values are added to a put request for
 * use with Remote KVStore api.
 */
public final class PutRequestDocumentWriter implements DocumentWriter {

  @VisibleForTesting final Map<IndexKey, List<Double>> doubleMap = new HashMap<>();

  @VisibleForTesting final Map<IndexKey, List<Integer>> integerMap = new HashMap<>();

  @VisibleForTesting final Map<IndexKey, List<String>> stringMap = new HashMap<>();

  @VisibleForTesting final Map<IndexKey, List<Long>> longMap = new HashMap<>();

  @VisibleForTesting final Map<IndexKey, List<byte[]>> byteArrayMap = new HashMap<>();

  @Override
  public void write(IndexKey key, Double value) {
    addEntries(doubleMap, key, value);
  }

  @Override
  public void write(IndexKey key, Integer value) {
    addEntries(integerMap, key, value);
  }

  @Override
  public void write(IndexKey key, Long value) {
    addEntries(longMap, key, value);
  }

  @Override
  public void write(IndexKey key, byte[]... values) {
    addEntries(byteArrayMap, key, values);
  }

  @Override
  public void write(IndexKey key, String... values) {
    addEntries(stringMap, key, values);
  }

  public void toPutRequest(RemoteDataStoreProtobuf.PutRequest.Builder putRequest) {
    for (Map.Entry<IndexKey, List<Double>> entry : doubleMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        putRequest.addIndexFields(
            RemoteDataStoreProtobuf.PutRequestIndexField.newBuilder()
                .setKey(RemoteDataStoreUtils.toPutRequestIndexKey(entry.getKey()))
                .addAllValueDouble(entry.getValue())
                .build());
      }
    }

    for (Map.Entry<IndexKey, List<Integer>> entry : integerMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        putRequest.addIndexFields(
            RemoteDataStoreProtobuf.PutRequestIndexField.newBuilder()
                .setKey(RemoteDataStoreUtils.toPutRequestIndexKey(entry.getKey()))
                .addAllValueInt32(entry.getValue())
                .build());
      }
    }

    for (Map.Entry<IndexKey, List<Long>> entry : longMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        putRequest.addIndexFields(
            RemoteDataStoreProtobuf.PutRequestIndexField.newBuilder()
                .setKey(RemoteDataStoreUtils.toPutRequestIndexKey(entry.getKey()))
                .addAllValueInt64(entry.getValue())
                .build());
      }
    }

    for (Map.Entry<IndexKey, List<String>> entry : stringMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        putRequest.addIndexFields(
            RemoteDataStoreProtobuf.PutRequestIndexField.newBuilder()
                .setKey(RemoteDataStoreUtils.toPutRequestIndexKey(entry.getKey()))
                .addAllValueString(entry.getValue())
                .build());
      }
    }

    for (Map.Entry<IndexKey, List<byte[]>> entry : byteArrayMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        putRequest.addIndexFields(
            RemoteDataStoreProtobuf.PutRequestIndexField.newBuilder()
                .setKey(RemoteDataStoreUtils.toPutRequestIndexKey(entry.getKey()))
                .addAllValueBytes(
                    entry.getValue().stream()
                        .map(UnsafeByteOperations::unsafeWrap)
                        .collect(Collectors.toList()))
                .build());
      }
    }
  }

  private <V> void addEntries(Map<IndexKey, List<V>> targetMap, IndexKey key, V... values) {
    final List<V> nonNullValues =
        Arrays.stream(values).filter(v -> v != null).collect(Collectors.toList());

    if (nonNullValues.isEmpty()) {
      return;
    }

    List<V> existingValues = targetMap.get(key);
    if (existingValues == null) {
      existingValues = new ArrayList<>();
      targetMap.put(key, existingValues);
    }

    if (!key.canContainMultipleValues()) {
      Preconditions.checkState(
          nonNullValues.size() == 1,
          "Cannot add multiple values to an index key [%s] with a single value. Multiple values attempting to be added.",
          key.getIndexFieldName());
      Preconditions.checkState(
          existingValues.isEmpty(),
          "Cannot add multiple values to an index key [%s] with a single value. A value is already present at that key.",
          key.getIndexFieldName());
    }

    existingValues.addAll(nonNullValues);
  }
}
