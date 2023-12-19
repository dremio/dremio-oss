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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableDocument;
import com.dremio.datastore.api.IncrementCounter;
import com.dremio.datastore.api.options.KVStoreOptionUtility;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;

/**
 * An in memory store for testing purposes.
 */
class MapStore implements ByteStore {

  private final ConcurrentSkipListMap<byte[], VersionedEntry> map;
  private final String name;

  static class VersionedEntry {
    private final String tag;
    private final ByteBuffer value;

    VersionedEntry(String tag, ByteBuffer value) {
      this.tag = tag;
      this.value = value;
    }

    String getTag() {
      return tag;
    }

    ByteBuffer getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, value);
    }

    @Override
    public boolean equals(Object rhs) {
      if (!(rhs instanceof VersionedEntry)) {
        return false;
      }

      final VersionedEntry that = (VersionedEntry) rhs;
      return Objects.equals(tag, that.tag) &&
        Objects.equals(value, that.value);
    }
  }

  public MapStore(String name){
    this.map = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    this.name = name;
  }

  @Override
  public Document<byte[], byte[]> get(byte[] key, GetOption... options) {
    final VersionedEntry entry = map.get(key);
    return entry == null ? null : new ImmutableDocument.Builder<byte[], byte[]>()
      .setKey(key)
      .setValue(entry.getValue().array())
      .setTag(entry.getTag())
      .build();
  }

  @Override
  public Iterable<Document<byte[], byte[]>> get(List<byte[]> keys, GetOption... options) {
    List<Document<byte[], byte[]>> values = new ArrayList<>();
    for(byte[] key : keys){
      values.add(get(key, options));
    }
    return values;
  }

  @Override
  public Document<byte[], byte[]> put(byte[] key, byte[] v, PutOption... options) {
    Preconditions.checkNotNull(v);
    final String newTag = ByteStore.generateTagFromBytes(v);
    map.put(key, new VersionedEntry(newTag, ByteBuffer.wrap(v)));
    return new ImmutableDocument.Builder<byte[], byte[]>()
      .setKey(key)
      .setValue(v)
      .setTag(newTag)
      .build();
  }

  @Override
  public boolean contains(byte[] key, ContainsOption... options) {
    return map.containsKey(key);
  }

  @Override
  public void delete(byte[] key, DeleteOption... options) {
    map.remove(key);
  }

  private Iterable<Document<byte[], byte[]>> toIterableDocs(Map<byte[], VersionedEntry> m) {
    return Iterables.transform(ImmutableMap.copyOf(m).entrySet(), TRANSFORMER);
  }

  @Override
  public Iterable<Document<byte[], byte[]>> find(FindByRange<byte[]> find, FindOption... options) {

    final boolean startIsNull = find.getStart() == null;
    final boolean endIsNull = find.getEnd() == null;
    if (startIsNull || endIsNull) {
      if (!endIsNull) {
        return toIterableDocs(map.headMap(find.getEnd(), find.isEndInclusive()));
      }

      if (!startIsNull) {
        return toIterableDocs(map.tailMap(find.getStart(), find.isStartInclusive()));
      }

      // only case left is both are null
      return toIterableDocs(map);
    }

    return toIterableDocs(map.subMap(find.getStart(), find.isStartInclusive(), find.getEnd(), find.isEndInclusive()));
  }

  @Override
  public void bulkIncrement(Map<byte[], List<IncrementCounter>> keysToIncrement, IncrementOption option) {
    throw new UnsupportedOperationException("Bulk increment operation is not supported.");
  }

  @Override
  public void bulkDelete(List<byte[]> keysToDelete) {
    throw new UnsupportedOperationException("Bulk delete operation is not supported.");
  }

  private static Function<Entry<byte[], VersionedEntry>, Document<byte[], byte[]>> TRANSFORMER = new Function<Entry<byte[], VersionedEntry>, Document<byte[], byte[]>>(){

    @Override
    public Document<byte[], byte[]> apply(final Entry<byte[], VersionedEntry> input) {
      return new Document<byte[], byte[]>(){

        @Override
        public byte[] getKey() {
          return input.getKey();
        }

        @Override
        public byte[] getValue() {
          if(input.getValue() != null){
            return input.getValue().getValue().array();
          } else {
            return null;
          }
        }

        @Override
        public String getTag() {
          return input.getValue().getTag();
        }
      };
    }};


  @Override
  public Iterable<Document<byte[], byte[]>> find(FindOption... options) {
    return toIterableDocs(map);
  }

  @Override
  public void close() throws Exception {
    map.clear();
  }

  @Override
  public void deleteAllValues() throws IOException {
    map.clear();
  }

  @Override
  public Document<byte[], byte[]> validateAndPut(byte[] key, byte[] newValue, VersionOption.TagInfo versionInfo, PutOption... options) {
    Preconditions.checkNotNull(newValue);
    Preconditions.checkNotNull(versionInfo);
    Preconditions.checkArgument(versionInfo.hasCreateOption() || versionInfo.getTag() != null);
    KVStoreOptionUtility.checkIndexPutOptionIsNotUsed(options);

    final Document<byte[], byte[]> oldEntry = get(key);

    // Validity check fails if the CREATE option is used but there is an existing entry,
    // or there's an existing entry and the tag doesn't match.
    if ((versionInfo.hasCreateOption() && oldEntry != null) || (!versionInfo.hasCreateOption() && oldEntry == null)
      || (versionInfo.getTag() != null && !versionInfo.getTag().equals(oldEntry.getTag()))) {
      return null;
    }

    final boolean succeeded;
    final String newTag = ByteStore.generateTagFromBytes(newValue);
    if (oldEntry == null) {
      succeeded = map.putIfAbsent(key, new VersionedEntry(newTag, ByteBuffer.wrap(newValue))) == null;
    } else {
      final byte[] oldValue = oldEntry.getValue();
      succeeded = map.replace(key, new VersionedEntry(oldEntry.getTag(), ByteBuffer.wrap(oldValue)), new VersionedEntry(newTag, ByteBuffer.wrap(newValue)));
    }

    if (succeeded) {
      return new ImmutableDocument.Builder<byte[], byte[]>()
        .setKey(key)
        .setValue(newValue)
        .setTag(newTag)
        .build();
    }

    return null;
  }

  @Override
  public boolean validateAndDelete(byte[] key, VersionOption.TagInfo versionInfo, DeleteOption... options) {
    Preconditions.checkNotNull(versionInfo);
    Preconditions.checkArgument(!versionInfo.hasCreateOption());
    Preconditions.checkNotNull(versionInfo.getTag());
    final Document<byte[], byte[]> oldEntry = get(key);

    // An entry is expected and the tags must match.
    if (oldEntry == null || !versionInfo.getTag().equals(oldEntry.getTag())) {
      return false;
    }

    return map.remove(key, new VersionedEntry(oldEntry.getTag(), ByteBuffer.wrap(oldEntry.getValue())));
  }

  @Override
  public KVAdmin getAdmin() {
    return new MapKVAdmin();
  }

  @Override
  public String getName() {
    return name;
  }

  private final class MapKVAdmin extends KVAdmin {

    @Override
    public String getStats() {
      StringBuilder sb = new StringBuilder();
      sb.append(name);
      sb.append('\n');
      sb.append("\tmap store stats");
      sb.append('\n');
      sb.append("\t\t* total records: ");
      sb.append(map.size());
      sb.append('\n');
      return sb.toString();
    }

  }
}
