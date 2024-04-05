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

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.format.compound.KeyPair;
import com.dremio.datastore.generator.ByteContainerStoreGenerator;
import org.junit.Test;

/** Tests that invalid key formats are not supported. */
public class TestLegacyInvalidKeyType {
  @Test(expected = DatastoreException.class)
  public void testByteFormatInvalidKey() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<byte[], String> builder =
        new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
            TestInvalidKeyType.DummyStoreBuilder::new);
    builder.keyFormat(Format.ofBytes());
  }

  @Test(expected = DatastoreException.class)
  public void testWrappedByteFormatInvalidKey() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<
            ByteContainerStoreGenerator.ByteContainer, String>
        builder =
            new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
                TestInvalidKeyType.DummyStoreBuilder::new);

    builder.keyFormat(
        Format.wrapped(
            ByteContainerStoreGenerator.ByteContainer.class,
            ByteContainerStoreGenerator.ByteContainer::getBytes,
            ByteContainerStoreGenerator.ByteContainer::new,
            Format.ofBytes()));
  }

  @Test(expected = DatastoreException.class)
  public void testCompoundKeyNotPermittedKVStore() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<KeyPair<String, String>, String> builder =
        new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
            TestInvalidKeyType.DummyStoreBuilder::new);

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.valueFormat(Format.ofString());
    builder.build();
  }

  @Test(expected = DatastoreException.class)
  public void testCompoundKeyNotPermittedIndexedKVStore() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<KeyPair<String, String>, String> builder =
        new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
            TestInvalidKeyType.DummyStoreBuilder::new);

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.valueFormat(Format.ofString());
    builder.buildIndexed(new TestInvalidKeyType.DummyDocumentConverter<>());
  }

  @Test
  public void testCompoundKeyPermittedKVStore() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<KeyPair<String, String>, String> builder =
        new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
            TestInvalidKeyType.DummyStoreBuilder::new);

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.permitCompoundKeys(true);
    builder.valueFormat(Format.ofString());
    builder.build();
  }

  @Test
  public void testCompoundKeyPermittedIndexedKVStore() {
    final LegacyKVStoreProvider.LegacyStoreBuilder<KeyPair<String, String>, String> builder =
        new LegacyKVStoreProviderAdapter.LegacyStoreBuilderAdapter<>(
            TestInvalidKeyType.DummyStoreBuilder::new);

    builder.keyFormat(Format.ofCompoundFormat("k1", Format.ofString(), "k2", Format.ofString()));
    builder.permitCompoundKeys(true);
    builder.valueFormat(Format.ofString());
    builder.buildIndexed(new TestInvalidKeyType.DummyDocumentConverter<>());
  }
}
