/*
 * Copyright (C) 2017 Dremio Corporation
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

import static java.lang.String.format;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * KVStore implementation that implements optimistic concurrency control.
 *
 * @param <VALUE>
 */
class OCCStore<KEY, VALUE> implements CoreKVStore<KEY, VALUE> {

  private final CoreKVStore<KEY, VALUE> store;
  private final boolean disableValidation;

  public OCCStore(CoreKVStore<KEY, VALUE> store, boolean disableValidation) {
    this.store = store;
    this.disableValidation = disableValidation;
  }

  protected CoreKVStore<KEY, VALUE> getStore(){
    return store;
  }

  @Override
  public KVStoreTuple<KEY> newKey() {
    return store.newKey();
  }

  @Override
  public KVStoreTuple<VALUE> newValue() {
    return store.newValue();
  }

  @Override
  public boolean contains(KVStoreTuple<KEY> key) {
    return store.contains(key);
  }

  @Override
  public KVStoreTuple<VALUE> get(KVStoreTuple<KEY> key) {
    return store.get(key);
  }

  @Override
  public Iterable<Map.Entry<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find() {
    return store.find();
  }

  @Override
  public void put(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> newValue) {
    Long previousVersion = newValue.incrementVersion();
    Long newVersion = newValue.getVersion();

    if (newVersion == null) {
      throw new IllegalArgumentException("missing version in " + newValue);
    }

    if(disableValidation){
      store.put(key, newValue);
    } else {
      final KVStoreTuple<VALUE> previousValue = get(key);
      if (isValid(previousVersion, previousValue)) {
        store.checkAndPut(key, previousValue, newValue);
      } else {
        final String expectedAction = previousVersion == null ? "create" : "update version " + previousVersion;
        final String previousValueDesc = previousValue.isNull()? "no previous version" : "previous version " + previousValue.getVersion();
        throw new ConcurrentModificationException(format("tried to %s, found %s", expectedAction, previousValueDesc));
      }
    }
  }

  @Override
  public void delete(KVStoreTuple<KEY> key, long previousVersion) {
    if (disableValidation) {
      store.delete(key);
    } else {
      final KVStoreTuple<VALUE> previousValue = get(key);
      if (isValid(previousVersion, previousValue)) {
        store.checkAndDelete(key, previousValue);
      } else {
        final String previousValueDesc = previousValue.isNull()? "no previous version"
            : "previous version " + previousValue.getVersion();
        throw new ConcurrentModificationException(
            format("tried to delete version %s, found %s", previousVersion, previousValueDesc));
      }
    }
  }

  @Override
  public List<KVStoreTuple<VALUE>> get(List<KVStoreTuple<KEY>> keys) {
    return store.get(keys);
  }

  @Override
  public boolean checkAndPut(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> oldValue, KVStoreTuple<VALUE> newValue) {
    return store.checkAndPut(key, oldValue, newValue);
  }

  @Override
  public void delete(KVStoreTuple<KEY> key) {
    store.delete(key);
  }

  @Override
  public boolean checkAndDelete(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> value) {
    return store.checkAndDelete(key, value);
  }

  @Override
  public Iterable<Entry<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(FindByRange<KVStoreTuple<KEY>> find) {
    return store.find(find);
  }

  private boolean isValid(Long version, KVStoreTuple<VALUE> value){
    if (version == null || value.isNull()) {
      return version == null && value.isNull();
    }
    return version.equals(value.getVersion());
  }

}

