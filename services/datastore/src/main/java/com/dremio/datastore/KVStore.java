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

import java.util.List;
import java.util.Map;

/**
 * A sorted key value store abstraction
 *
 * @param <K> the Key type
 * @param <V> the value type
 */
public interface KVStore<K, V> {

  /**
   * Return the value associated with the key, or {@code null} if no such value exists.
   *
   * @param key the key to use to look for the value
   * @return the associated value, or {@code null if not found}
   */
  V get(K key);


  /**
   * Get a list of values for each of the keys provided. List is in same order
   * as original request. Possibly more efficient that looping over the keys in
   * application code depending on the underlying storage system.
   *
   * @param keys
   *          Keys to retrieve.
   * @return list of values with parallel indices to the keys requested. If
   *         value is not found, the list has a null.
   */
  List<V> get(List<K> keys);

  /**
   * Save the provided value under the key. If the store already contains a value
   * associated with the key, the old value is discarded and replaced by the new value.
   *
   * @param key the key to save the value under
   * @param v the value to save, can not be null.
   * @throws NullPointerException when value is null.
   */
  void put(K key, V v);

  /**
   * Replace old value with new value atomically. If key is not associated with old value then return false.
   * @param key key to save the value under.
   * @param oldValue old value that must match with value associated with key.
   *                 old value can be null(put if key doesn't exist in key value store).
   * @param newValue new value to save.
   * @return true if old value is replaced by new value, false otherwise.
   * @throws NullPointerException when value is null.
   */
  boolean checkAndPut(K key, V oldValue, V newValue);

  /**
   * Indicate if the store contains an entry associated with the key. Return {@code true}
   * if such a key exist, {@code false} otherwise.
   *
   * @param key the key to look for
   * @return true if present, false otherwise
   */
  boolean contains(K key);

  /**
   * Remove the key, and the associated value from the store. If no such key exist, this
   * method does nothing.
   *
   * @param key the key to remove.
   */
  void delete(K key);


  /**
   * Delete the value at the provided key if the current value is equal to the provided value.
   * @param key The key for deletion
   * @param value The expected value at that key.
   * @return Whether or not the delete succeeded.
   */
  boolean checkAndDelete(K key, V value);


  /**
   * Return a iterable of keys & values for any key within the provided Range
   *
   * <p>
   * This function returns a map of key/value present in the store for key comprised
   * between start and end. Both start and end are not inclusive.
   * <p>
   * Although the returned type is not {@code java.util.SortedMap}, keys are returned in the
   * same order as the store.
   * <p>
   * The returned map is independent of the store, so changes to the returned map will not
   * be reflected into the store.
   *
   * @param start the start key
   * @param includeStart include rows matching start key
   * @param end the end key
   * @param includeEnd include rows matching end key
   * @return the sub map. Cannot be null
   */
  Iterable<Map.Entry<K, V>> find(FindByRange<K> find);

  /**
   * Returns all the entries stored in the store. Iterators returned from this
   * iterable are unmodifiable.
   *
   * @return a iterable containing all the store entries
   */
  Iterable<Map.Entry<K, V>> find();


  /**
   * Delete a key with a particular version. Throw ConccurentModificationException if failure occurs.
   *
   * @param key Key to delete.
   * @param previousVersion Previous version to be deleted.
   */
  void delete(K key, long previousVersion);

  /**
   * To validate that the previous value is the correct version (for example)
   *
   * @param <V> the Value type
   */
  interface Validator<V> {
    boolean isValid(V value);
  }

  /**
   * Get an administrative interface for this store.
   * @return Admin interface.
   */
  KVAdmin getAdmin();


  /**
   * Configuration for finding values by a key range.
   *
   * @param <K>
   */
  public static class FindByRange<K> {
    private K start;
    private boolean startInclusive;
    private K end;
    private boolean endInclusive;

    public FindByRange() {
      super();
    }

    public FindByRange(K start, boolean startInclusive, K end, boolean endInclusive) {
      this.start = start;
      this.startInclusive = startInclusive;
      this.end = end;
      this.endInclusive = endInclusive;
    }

    public FindByRange<K> setStart(K start, boolean inclusive) {
      this.start = start;
      this.startInclusive = inclusive;
      return this;
    }

    public FindByRange<K> setEnd(K end, boolean inclusive) {
      this.end = end;
      this.endInclusive = inclusive;
      return this;
    }

    public K getStart() {
      return start;
    }

    public boolean isStartInclusive() {
      return startInclusive;
    }

    public K getEnd() {
      return end;
    }

    public boolean isEndInclusive() {
      return endInclusive;
    }

  }

}
