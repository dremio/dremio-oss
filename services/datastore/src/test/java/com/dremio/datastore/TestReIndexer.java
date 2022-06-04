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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.format.Format;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.datastore.indexed.LuceneSearchIndex;

/**
 * Test re-indexer.
 */
public class TestReIndexer {
  private static final String storeName = "test-store";
  private static final byte[] one = "one".getBytes(StandardCharsets.UTF_8);
  private static final byte[] two = "two".getBytes(StandardCharsets.UTF_8);

  private static final IndexKey indexKey = IndexKey.newBuilder("test", "TEST", String.class).setStored(true)
    .build();

  private static class TestConverter implements DocumentConverter<String, String> {

    @Override
    public void convert(DocumentWriter writer, String key, String record) {
      writer.write(indexKey, key);
    }

    @Override
    public Integer getVersion() {
      return 0;
    }


  }

  private static ReIndexer reIndexer;
  private static IndexManager indexManager;
  private static CoreIndexedStore<String, String> store;

  @BeforeClass
  public static void setup() {
    indexManager = mock(IndexManager.class);
    store = mock(CoreIndexedStore.class);

    final StoreBuilderHelper<String, String> helper = new StoreBuilderHelper<String, String>()
      .name(storeName)
      .documentConverter(new TestConverter())
      .keyFormat(Format.ofString())
      .valueFormat(Format.ofString());

    reIndexer = new ReIndexer(indexManager,
        Collections.singletonMap(storeName, new CoreStoreProviderImpl.StoreWithId<String, String>(helper, store)));
  }

  @Test
  public void add() throws Exception {
    LuceneSearchIndex index = mock(LuceneSearchIndex.class);
    final boolean[] added = {false};
    doAnswer(invocation -> {
      added[0] = true;
      return null;
    }).when(index).update(any(Term.class), any(Document.class));
    when(indexManager.getIndex(same(storeName)))
        .thenReturn(index);

    reIndexer.put(storeName, one, two);

    assertTrue(added[0]);
  }

  @Test
  public void delete() throws Exception {
    LuceneSearchIndex index = mock(LuceneSearchIndex.class);
    final boolean[] deleted = {false};
    doAnswer(invocation -> {
      deleted[0] = true;
      return null;
    }).when(index).deleteDocuments(any(Term.class));
    when(indexManager.getIndex(same(storeName)))
        .thenReturn(index);

    reIndexer.delete(storeName, one);

    assertTrue(deleted[0]);
  }
}
