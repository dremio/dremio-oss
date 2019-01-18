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

/**
 * CoreKVStore which uses tuple for keys and values.
 */
public interface CoreKVStore<K, V> extends KVStore<KVStoreTuple<K>, KVStoreTuple<V>> {

  /**
   * Create a tuple for key. This allows rpc handler to convert bytes to objects.
   * @return empty KVStore tuple for key
   */
  KVStoreTuple<K> newKey();

  /**
   * Create a tuple for value. This allows rpc handler to convert bytes to object.
   * @return empty KVStore tuple for value
   */
  KVStoreTuple<V> newValue();

  /**
   * Validate the currently stored value before updating the store
   *
   * @param key the key
   * @param newValue the new value
   * @param validator a ValueValidator that ensures that the current item stored in the store for the key is valid
   * @return if the validation succeeded or not
   */
  boolean validateAndPut(KVStoreTuple<K> key, KVStoreTuple<V> newValue, ValueValidator<V> validator);

  /**
   * Validate the currently stored value before removing from the store
   *
   * @param key the key
   * @param validator a ValueValidator that ensures that the current item stored in the store for the key is valid
   * @return if the validation succeeded or not
   */
  boolean validateAndDelete(KVStoreTuple<K> key, ValueValidator<V> validator);

  /**
   * Value validator
   */
  interface ValueValidator<V> {
    boolean validate(KVStoreTuple<V> oldValue);
  }
}
