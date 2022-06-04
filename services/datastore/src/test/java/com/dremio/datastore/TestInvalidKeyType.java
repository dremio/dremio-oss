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

import org.junit.Test;

import com.dremio.datastore.api.AbstractStoreBuilder;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;

/**
 * Tests that invalid key formats are not supported.
 */
public class TestInvalidKeyType {
  static class DummyStoreBuilder<K, V> extends AbstractStoreBuilder<K, V> {
    @Override
    protected KVStore<K, V> doBuild() {
      return null;
    }

    @Override
    protected IndexedStore<K, V> doBuildIndexed(DocumentConverter<K, V> documentConverter) {
      return null;
    }
  }

  static class DummyDocumentConverter<K, V> implements DocumentConverter<K, V> {
    @Override
    public void convert(DocumentWriter writer, K key, V record) {

    }

    @Override
    public Integer getVersion() {
      return 0;
    }
  }

  @Test(expected = DatastoreException.class)
  public void testByteFormatInvalidKey() {
    final KVStoreProvider.StoreBuilder<byte[], String> builder = new DummyStoreBuilder<>();
    builder.keyFormat(Format.ofBytes());
  }

  @Test(expected = DatastoreException.class)
  public void testWrappedByteFormatInvalidKey() {
    final KVStoreProvider.StoreBuilder<ByteContainerStoreGenerator.ByteContainer, String> builder =
      new DummyStoreBuilder<>();

    builder.keyFormat(Format.wrapped(
      ByteContainerStoreGenerator.ByteContainer.class,
      ByteContainerStoreGenerator.ByteContainer::getBytes,
      ByteContainerStoreGenerator.ByteContainer::new, Format.ofBytes()));
  }

  @Test(expected = DatastoreException.class)
  public void testCompoundKeyNotPermittedKVStore() {
    final KVStoreProvider.StoreBuilder<KeyPair<String, String>, String> builder =
      new DummyStoreBuilder<>();

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.valueFormat(Format.ofString());
    builder.build();
  }

  @Test(expected = DatastoreException.class)
  public void testCompoundKeyNotPermittedIndexedKVStore() {
    final KVStoreProvider.StoreBuilder<KeyPair<String, String>, String> builder =
      new DummyStoreBuilder<>();

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.valueFormat(Format.ofString());
    builder.buildIndexed(new DummyDocumentConverter<>());
  }

  @Test
  public void testCompoundKeyPermittedKVStore() {
    final KVStoreProvider.StoreBuilder<KeyPair<String, String>, String> builder =
      new DummyStoreBuilder<>();

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.permitCompoundKeys(true);
    builder.valueFormat(Format.ofString());
    builder.build();
  }

  @Test
  public void testCompoundKeyPermittedIndexedKVStore() {
    final KVStoreProvider.StoreBuilder<KeyPair<String, String>, String> builder =
      new DummyStoreBuilder<>();

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.permitCompoundKeys(true);
    builder.valueFormat(Format.ofString());
    builder.buildIndexed(new DummyDocumentConverter<>());
  }

}
