/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.google.common.base.Throwables;

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
  public KVAdmin getAdmin() {
    return store.getAdmin();
  }

  @Override
  public KVStoreTuple<VALUE> get(KVStoreTuple<KEY> key) {
    KVStoreTuple<VALUE> value = store.get(key);
    checkAndUpdateToStringVersion(key,value);
    return value;
  }

  @Override
  public Iterable<Map.Entry<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find() {
    return () -> StreamSupport.stream(store.find().spliterator(), false)
      .peek(entry -> checkAndUpdateToStringVersion(entry.getKey(), entry.getValue()))
      .iterator();
  }

  @Override
  public void put(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> newValue) {
    try (RollbackCloseable rollback = new RollbackCloseable()) {
      // run pre-commit before we increment version
      rollback.add(newValue.preCommit());

      final String previousVersion = newValue.incrementVersion();
      final String newVersion = newValue.getTag();

      if (newVersion == null) {
        throw new IllegalArgumentException("missing version in " + newValue);
      }

      rollback.add(() -> {
        // manually rollback version
        newValue.setTag(previousVersion);
      });

      if (disableValidation) {
        store.put(key, newValue);
      } else {
        boolean valid = store.validateAndPut(key, newValue,
          (KVStoreTuple<VALUE> oldValue) -> {
            // check if the previous version matches what is currently stored
            return isValid(previousVersion, oldValue);
         }
       );

        if (!valid) {
          final KVStoreTuple<VALUE> currentValue = store.get(key);

          final String expectedAction = previousVersion == null ? "create" : "update version " + previousVersion;
          final String previousValueDesc = currentValue.isNull() ? "no previous version" : "previous version " + currentValue.getTag();
          throw new ConcurrentModificationException(format("tried to %s, found %s", expectedAction, previousValueDesc));
        }
      }

      rollback.commit();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean validateAndPut(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> newValue, ValueValidator<VALUE> validator) {
    return store.validateAndPut(key, newValue, validator);
  }

  @Override
  public void delete(KVStoreTuple<KEY> key, String previousVersion) {
    if (disableValidation) {
      store.delete(key);
    } else {
      boolean valid = store.validateAndDelete(key,
        (KVStoreTuple<VALUE> oldValue) -> {
          // check if the previous version matches what is currently stored
          return isValid(previousVersion, oldValue);
        }
      );

      if (!valid) {
        final KVStoreTuple<VALUE> currentValue = store.get(key);

        final String previousValueDesc = currentValue.isNull()? "no previous version"
            : "previous version " + currentValue.getTag();
        throw new ConcurrentModificationException(
            format("tried to delete version %s, found %s", previousVersion, previousValueDesc));
      }
    }
  }

  @Override
  public boolean validateAndDelete(KVStoreTuple<KEY> key, ValueValidator<VALUE> validator) {
    return store.validateAndDelete(key, validator);
  }

  @Override
  public List<KVStoreTuple<VALUE>> get(List<KVStoreTuple<KEY>> keys) {
    return keys.stream().map(this::get).collect(Collectors.toList());
  }

  @Override
  public void delete(KVStoreTuple<KEY> key) {
    store.delete(key);
  }

  @Override
  public Iterable<Entry<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(FindByRange<KVStoreTuple<KEY>> find) {
    return () -> StreamSupport.stream(store.find(find).spliterator(), false)
      .peek(entry -> checkAndUpdateToStringVersion(entry.getKey(), entry.getValue()))
      .iterator();
  }

  private boolean isValid(String version, KVStoreTuple<VALUE> value){
    if (version == null || value.isNull()) {
      return version == null && value.isNull();
    }
    return version.equals(value.getTag());
  }

  private void checkAndUpdateToStringVersion(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> value){
    if(value == null || value.isNull()){
      return;
    }
    // set a string version if it doesn't have one
    value.inlineUpgradeToStringTag();
  }
}

