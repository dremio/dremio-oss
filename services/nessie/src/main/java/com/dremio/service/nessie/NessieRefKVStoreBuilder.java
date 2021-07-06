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
package com.dremio.service.nessie;

import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.TagName;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreCreationFunction;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Creates the NamedReference KV store for Nessie.
 */
public class NessieRefKVStoreBuilder implements KVStoreCreationFunction<NamedRef, Hash> {
  static final String TABLE_NAME = "nessieRef";
  static final String BRANCH_PREFIX = "B|";
  static final String TAG_PREFIX = "T|";

  @Override
  public KVStore<NamedRef, org.projectnessie.versioned.Hash> build(StoreBuildingFactory factory) {
    return factory.<NamedRef, Hash>newStore()
        .name(TABLE_NAME)
        .keyFormat(Format.wrapped(NamedRef.class, NessieRefKVStoreBuilder::encodeNamedRef, NessieRefKVStoreBuilder::decodeNamedRef, Format.ofString()))
        .valueFormat(Format.wrapped(Hash.class, Hash::asString, Hash::of, Format.ofString()))
        .build();
  }

  static String encodeNamedRef(NamedRef namedRef) {
    if (namedRef instanceof BranchName) {
      return BRANCH_PREFIX + namedRef.getName();
    } else if (namedRef instanceof TagName) {
      return TAG_PREFIX + namedRef.getName();
    } else {
      throw new IllegalArgumentException();
    }
  }

  static NamedRef decodeNamedRef(String encoded) {
    if (encoded.startsWith(BRANCH_PREFIX)) {
      return BranchName.of(encoded.substring(BRANCH_PREFIX.length()));
    } else if (encoded.startsWith(TAG_PREFIX)) {
      return TagName.of(encoded.substring(TAG_PREFIX.length()));
    } else {
      throw new IllegalArgumentException();
    }
  }
}
