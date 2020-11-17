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

import java.util.ConcurrentModificationException;
import java.util.List;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.options.VersionOption;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * KVStore created by CoreStoreProvider.
 * TODO : replace preconditions with assert
 */
public class CoreKVStoreImpl<KEY, VALUE> implements CoreKVStore<KEY, VALUE> {

  private final ByteStore rawStore;
  private final Serializer<KEY, byte[]> keySerializer;
  private final Serializer<VALUE, byte[]> valueSerializer;

  private final Function<KVStoreTuple<KEY>, byte[]> keyToBytes = KVStoreTuple::getSerializedBytes;

  public CoreKVStoreImpl(ByteStore rawStore,
                         Serializer<KEY, byte[]> keySerializer,
                         Serializer<VALUE, byte[]> valueSerializer) {
    this.rawStore = rawStore;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public KVStoreTuple<KEY> newKey() {
    return new KVStoreTuple<>(keySerializer);
  }

  @Override
  public KVStoreTuple<VALUE> newValue() {
    return new KVStoreTuple<>(valueSerializer);
  }

  @Override
  public Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>> get(KVStoreTuple<KEY> key, GetOption... options) {
    return fromDocument(rawStore.get(key.getSerializedBytes(), options));
  }

  @Override
  public Iterable<Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> get(List<KVStoreTuple<KEY>> keys, GetOption... options) {
    final List<byte[]> convertedKeys = Lists.transform(keys, keyToBytes);
    final Iterable<Document<byte[], byte[]>> convertedResults = rawStore.get(convertedKeys, options);
    return Iterables.transform(convertedResults, this::fromDocument);
  }

  @Override
  public Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>> put(KVStoreTuple<KEY> key, KVStoreTuple<VALUE> value, PutOption... options) {
    final VersionOption.TagInfo tagInfo = VersionOption.getTagInfo(options);
    final Document<byte[], byte[]> result = (tagInfo.hasVersionOption()) ?
        rawStore.validateAndPut(key.getSerializedBytes(), value.getSerializedBytes(), tagInfo, options) :
        rawStore.put(key.getSerializedBytes(), value.getSerializedBytes());

    if (result == null) {
      final Document<byte[], byte[]> currentEntry = rawStore.get(key.getSerializedBytes());

      final String expectedAction = tagInfo.getTag() == null ? "create" : "update version " + tagInfo.getTag();
      final String previousValueDesc = currentEntry == null || Strings.isNullOrEmpty(currentEntry.getTag()) ?
        "no tag" : "tag " + currentEntry.getTag();
      throw new ConcurrentModificationException(String.format("Tried to %s, found %s", expectedAction, previousValueDesc));

    }

    return fromDocument(result);
  }

  @Override
  public boolean contains(KVStoreTuple<KEY> key, ContainsOption... options) {
    return rawStore.contains(key.getSerializedBytes(), options);
  }

  @Override
  public void delete(KVStoreTuple<KEY> key, DeleteOption... options) {
    final VersionOption.TagInfo tagInfo = VersionOption.getTagInfo(options);

    if (tagInfo.hasVersionOption()) {
      if (!rawStore.validateAndDelete(key.getSerializedBytes(), tagInfo, options)) {
        Document<byte[], byte[]> current = rawStore.get(key.getSerializedBytes());
        throw new ConcurrentModificationException(
          String.format("Cannot delete, expected tag %s but found tag %s",  tagInfo.getTag(), current.getTag()));
      }
    } else {
      rawStore.delete(key.getSerializedBytes(), options);
    }
  }

  @Override
  public Iterable<Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(FindByRange<KVStoreTuple<KEY>> find, FindOption... options) {
    final ImmutableFindByRange.Builder<byte[]> rangeBuilder = new ImmutableFindByRange.Builder<>();

    if (find.getStart() != null) {
      rangeBuilder.setStart(find.getStart().getSerializedBytes())
        .setIsStartInclusive(find.isStartInclusive());
    }

    if (find.getEnd() != null) {
      rangeBuilder.setEnd(find.getEnd().getSerializedBytes())
        .setIsEndInclusive(find.isEndInclusive());
    }

    final Iterable<Document<byte[], byte[]>> range = rawStore.find(rangeBuilder.build(), options);
    return Iterables.transform(range, this::fromDocument);
  }

  @Override
  public Iterable<Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>>> find(FindOption... options) {
    return Iterables.transform(rawStore.find(options), this::fromDocument);
  }

  @Override
  public String getName() {
    return rawStore.getName();
  }

  CoreKVStoreDocument fromDocument(Document<byte[], byte[]> input) {
    if (input == null) {
      return null;
    }
    return new CoreKVStoreDocument(input);
  }

  final class CoreKVStoreDocument implements Document<KVStoreTuple<KEY>, KVStoreTuple<VALUE>> {

    private final KVStoreTuple<KEY> key;
    private final KVStoreTuple<VALUE> value;
    private final String tag;

    private CoreKVStoreDocument(final Document<byte[], byte[]> input) {
      this.key = newKey().setSerializedBytes(input.getKey());
      this.value = newValue().setSerializedBytes(input.getValue());
      this.tag = input.getTag();
    }

    @Override
    public KVStoreTuple<KEY> getKey() {
      return key;
    }

    @Override
    public KVStoreTuple<VALUE> getValue() {
      return value;
    }

    @Override
    public String getTag() {
      return tag;
    }
  }

  @Override
  public KVAdmin getAdmin() {
    return rawStore.getAdmin();
  }
}
