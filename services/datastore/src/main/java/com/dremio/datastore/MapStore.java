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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;

/**
 * An in memory store for testing purposes.
 */
class MapStore implements ByteStore {

  private final ConcurrentSkipListMap<byte[], ByteBuffer> map;

  public MapStore(){
    this.map = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
  }

  @Override
  public byte[] get(byte[] key) {
    final ByteBuffer arr = map.get(key);
    return arr == null ? null : arr.array();
  }

  @Override
  public List<byte[]> get(List<byte[]> keys) {
    List<byte[]> values = new ArrayList<>();
    for(byte[] key : keys){
      values.add(get(key));
    }
    return values;
  }

  @Override
  public void put(byte[] key, byte[] v) {
    Preconditions.checkNotNull(v);
    map.put(key, ByteBuffer.wrap(v));
  }

  @Override
  public boolean checkAndPut(byte[] key, byte[] oldValue, byte[] newValue) {
    Preconditions.checkNotNull(newValue);
    if(oldValue == null){
      return map.putIfAbsent(key, ByteBuffer.wrap(newValue)) == null;
    }else {
      return map.replace(key, ByteBuffer.wrap(oldValue), ByteBuffer.wrap(newValue));
    }
  }

  @Override
  public boolean contains(byte[] key) {
    return map.containsKey(key);
  }

  @Override
  public void delete(byte[] key) {
    map.remove(key);
  }

  @Override
  public boolean checkAndDelete(byte[] key, byte[] value) {
    Preconditions.checkNotNull(value);
    return map.remove(key, ByteBuffer.wrap(value));
  }

  @Override
  public Iterable<Entry<byte[], byte[]>> find(com.dremio.datastore.KVStore.FindByRange<byte[]> find) {
    return Iterables.transform(ImmutableMap.copyOf(map.subMap(find.getStart(), find.isStartInclusive(), find.getEnd(), find.isEndInclusive())).entrySet(), TRANSFORMER);
  }

  private static Function<Entry<byte[], ByteBuffer>, Entry<byte[], byte[]>> TRANSFORMER = new Function<Entry<byte[], ByteBuffer>, Entry<byte[], byte[]>>(){

    @Override
    public Entry<byte[], byte[]> apply(final Entry<byte[], ByteBuffer> input) {
      return new Entry<byte[], byte[]>(){

        @Override
        public byte[] getKey() {
          return input.getKey();
        }

        @Override
        public byte[] getValue() {
          if(input.getValue() != null){
            return input.getValue().array();
          } else {
            return null;
          }
        }

        @Override
        public byte[] setValue(byte[] value) {
          throw new UnsupportedOperationException();
        }

      };
    }};


  @Override
  public Iterable<Entry<byte[], byte[]>> find() {
    return Iterables.transform(ImmutableMap.copyOf(map).entrySet(), TRANSFORMER);
  }

  @Override
  public void delete(byte[] key, long previousVersion) {
    throw new UnsupportedOperationException("You must use a versioned store to delete by version.");
  }

  @Override
  public void close() throws Exception {
    map.clear();
  }

  @Override
  public void deleteAllValues() throws IOException {
    map.clear();
  }

}
