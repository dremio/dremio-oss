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
package com.dremio.service.nessie.upgrade.version040;

import java.io.IOException;
import java.io.StringReader;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreCreationFunction;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.nessie.upgrade.version040.model.NessieCommit;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

/**
 * Creates the KV store for Nessie.
 */
public class NessieCommitKVStoreBuilder implements KVStoreCreationFunction<String, NessieCommit> {
  static final String TABLE_NAME = "nessieCommit";
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public KVStore<String, NessieCommit> build(StoreBuildingFactory factory) {
    return factory.<String, NessieCommit>newStore()
      .name(TABLE_NAME)
      .keyFormat(Format.ofString())
      .valueFormat(Format.wrapped(
        NessieCommit.class,
        c -> null, // Not used
        NessieCommitKVStoreBuilder::stringToCommit,
        Format.ofString()))
      .build();
  }

  @VisibleForTesting
  static NessieCommit stringToCommit(String s) {
    try {
      return objectMapper.readValue(new StringReader(s), NessieCommit.class);
    } catch (IOException e) {
      throw new IllegalArgumentException();
    }
  }
}
