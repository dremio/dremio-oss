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

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreCreationFunction;
import com.dremio.datastore.api.StoreBuildingFactory;
import com.dremio.datastore.format.Format;

/**
 * Creates the NamedReference KV store for Nessie.
 */
public class NessieRefKVStoreBuilder implements KVStoreCreationFunction<NessieRefKVStoreBuilder.NamedRef, String> {
  static final String TABLE_NAME = "nessieRef";
  static final String BRANCH_PREFIX = "B|";
  static final String TAG_PREFIX = "T|";

  public abstract static class NamedRef {
    private final String name;

    protected NamedRef(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public abstract String encode();
  }

  public static class BranchRef extends NamedRef {
    public BranchRef(String name) {
      super(name);
    }

    @Override
    public String encode() {
      return String.format("B|%s", getName());
    }
  }

  public static class TagRef extends NamedRef {
    public TagRef(String name) {
      super(name);
    }

    @Override
    public String encode() {
      return String.format("T|%s", getName());
    }
  }

  @Override
  public KVStore<NessieRefKVStoreBuilder.NamedRef, String> build(StoreBuildingFactory factory) {
    return factory.<NessieRefKVStoreBuilder.NamedRef, String>newStore()
      .name(TABLE_NAME)
      .keyFormat(Format.wrapped(NessieRefKVStoreBuilder.NamedRef.class, k -> k.encode(), NessieRefKVStoreBuilder::decodeNamedRef, Format.ofString()))
      .valueFormat(Format.ofString())
      .build();
  }

  static NessieRefKVStoreBuilder.NamedRef decodeNamedRef(String encoded) {
    if (encoded.startsWith(BRANCH_PREFIX)) {
      return new BranchRef(encoded.substring(BRANCH_PREFIX.length()));
    } else if (encoded.startsWith(TAG_PREFIX)) {
      return new TagRef(encoded.substring(TAG_PREFIX.length()));
    } else {
      return null;
    }
  }
}
